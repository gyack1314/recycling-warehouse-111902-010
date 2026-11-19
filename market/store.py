"""持久化存储基元：DuckDB 元数据库、真实 Parquet、传递包读写与内容校验。"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .hashing import canonical_json, sha256_file, sha256_hex

MANIFEST_NAME = "manifest.json"
ACK_NAME = "ack.json"

PROVIDER_DDL = """
CREATE TABLE IF NOT EXISTS contracts (
    contract_hash TEXT PRIMARY KEY,
    contract_json TEXT NOT NULL,
    accepted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS views (
    contract_hash TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    view_fp TEXT NOT NULL,
    multiset_json TEXT NOT NULL,
    built_at TEXT NOT NULL,
    retired INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (contract_hash, snapshot_id)
);
CREATE TABLE IF NOT EXISTS generations (
    contract_hash TEXT PRIMARY KEY,
    generation INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    contract_hash TEXT NOT NULL,
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    from_view TEXT,
    to_view TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    package_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acked INTEGER NOT NULL DEFAULT 0,
    acked_at TEXT
);
"""

RECEIVER_DDL = """
CREATE TABLE IF NOT EXISTS accepted_contracts (
    contract_hash TEXT PRIMARY KEY,
    contract_json TEXT NOT NULL,
    accepted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger (
    delivery_id TEXT PRIMARY KEY,
    contract_hash TEXT NOT NULL,
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    from_view TEXT,
    to_view TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id INTEGER PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    seen_content_hash TEXT,
    stored_content_hash TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS conflict_seq START 1;
CREATE TABLE IF NOT EXISTS refused (
    delivery_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rounds (
    round_id INTEGER PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    from_view TEXT,
    to_view TEXT NOT NULL,
    added_rows INTEGER NOT NULL,
    removed_rows INTEGER NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS round_seq START 1;
CREATE TABLE IF NOT EXISTS current_state (
    contract_hash TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    view_fp TEXT NOT NULL,
    multiset_json TEXT NOT NULL,
    baseline_snapshot TEXT,
    updated_at TEXT NOT NULL
);
"""


def now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def open_db(path: Path) -> duckdb.DuckDBPyConnection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("PRAGMA threads=1")
    return con


def write_parquet(table: pa.Table, path: Path) -> None:
    """按表自身 schema 落真实 Parquet（定点 decimal 原样保留）。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class Package:
    root: Path
    manifest: dict[str, Any]

    @property
    def delivery_id(self) -> str:
        return self.manifest["delivery_id"]

    @property
    def content_hash(self) -> str:
        return self.manifest["content_hash"]

    def payload_path(self, name: str) -> Path | None:
        spec = self.manifest["payloads"].get(name)
        return None if spec is None else self.root / spec["file"]


def manifest_content_hash(manifest: Mapping[str, Any]) -> str:
    """清单内容指纹：覆盖除自身 content_hash 外全部字段（含载荷文件摘要）。"""
    body = {k: v for k, v in manifest.items() if k != "content_hash"}
    return sha256_hex(canonical_json(body))


def write_package(
    outbox: Path,
    manifest: Mapping[str, Any],
    payloads: Mapping[str, pa.Table | None],
) -> Package:
    """在交接目录写出一个自校验传递包（清单 + 载荷 Parquet + 摘要）。"""
    outbox = Path(outbox)
    root = outbox / manifest["delivery_id"]
    if root.exists():
        shutil.rmtree(root)
    (root / "payloads").mkdir(parents=True)

    payload_specs: dict[str, Any] = {}
    for name, table in payloads.items():
        if table is None or table.num_rows == 0:
            continue
        rel = f"payloads/{name}.parquet"
        write_parquet(table, root / rel)
        payload_specs[name] = {
            "file": rel,
            "sha256": sha256_file(root / rel),
            "rows": table.num_rows,
        }

    full = dict(manifest)
    full["payloads"] = payload_specs
    full["content_hash"] = manifest_content_hash(full)
    atomic_write_text(root / MANIFEST_NAME,
                      json.dumps(full, ensure_ascii=False, indent=2, sort_keys=True))
    return Package(root, full)


def read_package(package_dir: Path) -> Package:
    package_dir = Path(package_dir)
    manifest = json.loads((package_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    return Package(package_dir, manifest)


def verify_package_static(pkg: Package) -> None:
    """与账本无关的静态核验：清单指纹、载荷摘要、声明行数。"""
    from .errors import VerificationFailed
    recomputed = manifest_content_hash(pkg.manifest)
    if recomputed != pkg.manifest["content_hash"]:
        raise VerificationFailed(
            f"传递 {pkg.delivery_id} 清单内容指纹不符: 声明 {pkg.manifest['content_hash'][:12]} "
            f"实算 {recomputed[:12]}（清单或载荷被改动）"
        )
    for name, spec in pkg.manifest["payloads"].items():
        p = pkg.root / spec["file"]
        if not p.is_file():
            raise VerificationFailed(f"传递 {pkg.delivery_id} 缺少载荷 {name}")
        actual = sha256_file(p)
        if actual != spec["sha256"]:
            raise VerificationFailed(f"传递 {pkg.delivery_id} 载荷 {name} 摘要不符")
        rows = pq.ParquetFile(p).metadata.num_rows
        if rows != spec["rows"]:
            raise VerificationFailed(f"传递 {pkg.delivery_id} 载荷 {name} 行数不符")


def copy_package(src: Path, dst_root: Path) -> Package:
    """本地文件交接：整包复制到接收 in-box，不依赖网络。"""
    pkg = read_package(src)
    dst = Path(dst_root) / pkg.delivery_id
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    return read_package(dst)
