"""提供方共享仓。

职责：
- 接纳共享申请（契约），按契约把每个原始快照解释为投影/筛选后的行多重集；
- 生成基线快照、差量、完整替换（新基线代际）三类持久化传递包；
- 只保留有限旧版本：基线退休后无法再计算其差量时，协商完整替换；
- 账本落 DuckDB，重发只复制已持久化的包，不依赖进程内“已发送”变量。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from .contract import ShareContract
from .errors import MissingBaseline
from .hashing import (
    apply_delta,
    diff_multisets,
    multiset_hashes,
    row_hash,
    view_fingerprint,
)
from .store import (
    Package,
    PROVIDER_DDL,
    atomic_write_text,
    copy_package,
    now_iso,
    open_db,
    read_package,
    write_package,
    write_parquet,
)


class ProviderStore:
    def __init__(self, root, inputs_dir, versions_file):
        self.root = Path(root).resolve()
        self.inputs_dir = Path(inputs_dir).resolve()
        self.versions_file = Path(versions_file).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "packages").mkdir(exist_ok=True)
        (self.root / "views").mkdir(exist_ok=True)
        self.db_path = self.root / "provider.duckdb"
        self.con = open_db(self.db_path)
        self.con.execute(PROVIDER_DDL)
        self._versions = json.loads(self.versions_file.read_text(encoding="utf-8"))

    def close(self):
        self.con.close()

    # ---------- 契约 ----------
    def accept_application(self, application: dict[str, Any]) -> str:
        contract = ShareContract.from_dict(application["contract"])
        ch = contract.identity_hash()
        self.con.execute(
            "INSERT OR IGNORE INTO contracts VALUES (?, ?, ?)",
            (ch, json.dumps(contract.to_dict(), ensure_ascii=False, sort_keys=True),
             now_iso()),
        )
        self.con.execute(
            "INSERT OR IGNORE INTO generations(contract_hash, generation) VALUES (?, 1)",
            (ch,),
        )
        self.con.commit()
        return ch

    def get_contract(self, contract_hash: str) -> ShareContract:
        row = self.con.execute(
            "SELECT contract_json FROM contracts WHERE contract_hash=?", [contract_hash]
        ).fetchone()
        if row is None:
            raise ValueError(f"提供方未接纳契约 {contract_hash[:12]}…")
        return ShareContract.from_dict(json.loads(row[0]))

    def _generation(self, contract_hash: str) -> int:
        row = self.con.execute(
            "SELECT generation FROM generations WHERE contract_hash=?", [contract_hash]
        ).fetchone()
        if row is None:
            raise ValueError("契约尚未被接纳，无法生成传递")
        return int(row[0])

    # ---------- 视图 ----------
    def _source_files(self, snapshot_id: str) -> list[Path]:
        if snapshot_id not in self._versions:
            raise ValueError(f"版本清单中没有快照 {snapshot_id}")
        return [self.inputs_dir / name for name in self._versions[snapshot_id]]

    def build_view(self, contract: ShareContract, snapshot_id: str) -> dict[str, Any]:
        """把原始快照解释为共享视图多重集并持久化（幂等）。

        物理重排（路径/行组/顺序变化）不影响视图指纹：同内容重算得到相同 fp，
        不会制造任何“假删除”轮次。
        """
        ch = contract.identity_hash()
        existing = self.con.execute(
            "SELECT view_fp, multiset_json, retired FROM views "
            "WHERE contract_hash=? AND snapshot_id=?", [ch, snapshot_id]
        ).fetchone()
        rows = contract.project_files(self.con, self._source_files(snapshot_id))
        multiset = multiset_hashes(contract.columns, contract.types, rows)
        fp = view_fingerprint(multiset)
        view_dir = self.root / "views" / ch[:16]
        view_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = view_dir / f"{snapshot_id}.parquet"
        # 视图文件保留全部重复行（重数），仅按哈希做稳定排序，与原件物理布局无关
        ordered = sorted(rows, key=lambda r: row_hash(contract.columns, contract.types, r))
        write_parquet(contract.rows_to_arrow(ordered), parquet_path)

        if existing is not None:
            if existing[0] != fp:
                raise ValueError(
                    f"快照 {snapshot_id} 已构建且视图指纹变化，原件不可在原位被修改"
                )
            return {"snapshot_id": snapshot_id, "view_fp": fp,
                    "multiset": multiset, "rebuilt": True, "retired": bool(existing[2])}
        self.con.execute(
            "INSERT INTO views VALUES (?, ?, ?, ?, ?, 0)",
            (ch, snapshot_id, fp, json.dumps(multiset, sort_keys=True), now_iso()),
        )
        self.con.commit()
        return {"snapshot_id": snapshot_id, "view_fp": fp,
                "multiset": multiset, "rebuilt": False, "retired": False}

    def _load_view_row(self, contract_hash: str, snapshot_id: str):
        row = self.con.execute(
            "SELECT view_fp, multiset_json, retired FROM views "
            "WHERE contract_hash=? AND snapshot_id=?", [contract_hash, snapshot_id]
        ).fetchone()
        if row is None:
            raise MissingBaseline(f"提供方没有快照 {snapshot_id} 的共享视图，请先构建")
        return row

    def retire(self, contract_hash: str, snapshot_ids: Sequence[str]) -> dict[str, Any]:
        contract = self.get_contract(contract_hash)
        retired = []
        for sid in snapshot_ids:
            if sid not in self._versions:
                raise ValueError(f"版本清单中没有快照 {sid}，无法退休")
            existing = self.con.execute(
                "SELECT retired FROM views WHERE contract_hash=? AND snapshot_id=?",
                [contract_hash, sid],
            ).fetchone()
            if existing is None:
                # 有限保留策略：先按契约把视图构建落盘，再标记退休
                self.build_view(contract, sid)
            self.con.execute(
                "UPDATE views SET retired=1 WHERE contract_hash=? AND snapshot_id=?",
                [contract_hash, sid],
            )
            retired.append(sid)
        self.con.commit()
        return {"retired": retired}

    def open_baselines(self, contract_hash: str) -> list[dict[str, Any]]:
        return [
            {"snapshot_id": r[0], "view_fp": r[1]}
            for r in self.con.execute(
                "SELECT snapshot_id, view_fp FROM views "
                "WHERE contract_hash=? AND retired=0 ORDER BY built_at", [contract_hash]
            ).fetchall()
        ]

    # ---------- 传递生成 ----------
    def _rows_for_hashes(self, contract: ShareContract, snapshot_id: str,
                         hashes: dict[str, int]) -> list[dict[str, Any]]:
        """从已持久化的视图 Parquet 取行，按重数复制份数（重复行不去重）。"""
        ch = contract.identity_hash()
        view_path = self.root / "views" / ch[:16] / f"{snapshot_id}.parquet"
        all_rows = contract.project_files(self.con, [view_path])
        by_hash: dict[str, dict[str, Any]] = {}
        for row in all_rows:
            from .hashing import row_hash
            by_hash.setdefault(row_hash(contract.columns, contract.types, row), row)
        out: list[dict[str, Any]] = []
        for rh, n in hashes.items():
            if rh not in by_hash:
                raise MissingBaseline(f"基线缺少行 {rh[:12]}… 的内容")
            out.extend([dict(by_hash[rh]) for _ in range(n)])
        return out

    def _persist_package(self, manifest: dict[str, Any], payloads: dict) -> Package:
        delivery_id = manifest["delivery_id"]
        # write_package 会在给定根下再建 delivery_id 目录，这里根是 packages/
        pkg = write_package(self.root / "packages", manifest, payloads)
        self.con.execute(
            "INSERT OR REPLACE INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)",
            (pkg.delivery_id, manifest["contract_hash"], manifest["kind"],
             manifest["generation"], manifest.get("from_view"), manifest["to_view"],
             pkg.content_hash, str(pkg.root), now_iso()),
        )
        self.con.commit()
        return pkg

    def offer_snapshot(self, contract: ShareContract, snapshot_id: str) -> Package:
        """首轮基线（或退休协商后的全新基线由 offer_rebase 处理）。"""
        ch = contract.identity_hash()
        gen = self._generation(ch)
        view = self.build_view(contract, snapshot_id)
        delivery_id = f"{ch[:12]}-g{gen}-snap-{snapshot_id}"
        rows = self._rows_for_hashes(contract, snapshot_id, view["multiset"])
        manifest = {
            "manifest_version": 1,
            "delivery_id": delivery_id,
            "contract_id": contract.id,
            "contract_hash": ch,
            "kind": "snapshot",
            "generation": gen,
            "from_view": None,
            "to_view": view["view_fp"],
            "from_snapshot": None,
            "to_snapshot": snapshot_id,
            "multiset": dict(sorted(view["multiset"].items())),
            "counts": {"added": sum(view["multiset"].values()), "removed": 0},
            "created_at": now_iso(),
        }
        return self._persist_package(manifest, {"full": contract.rows_to_arrow(rows)})

    def offer_delta(self, contract: ShareContract, from_snapshot: str,
                    to_snapshot: str) -> Package:
        """生成两个开放基线之间的净变化（同代际差量）。"""
        ch = contract.identity_hash()
        gen = self._generation(ch)
        before_row = self._load_view_row(ch, from_snapshot)
        after = self.build_view(contract, to_snapshot)
        if bool(before_row[2]):
            raise MissingBaseline(
                f"差量基线 {from_snapshot} 已退休，提供方不再保证可计算该基线差量；"
                "应协商以完整替换重建基线"
            )
        before_fp, before_ms_raw, _ = before_row
        before_ms = json.loads(before_ms_raw)
        added, removed = diff_multisets(before_ms, after["multiset"])
        target_ms = apply_delta(before_ms, added, removed)
        target_fp = view_fingerprint(target_ms)
        if target_fp != after["view_fp"]:  # 内部自检
            raise ValueError("差量应用结果与目标视图指纹不一致")
        delivery_id = f"{ch[:12]}-g{gen}-delta-{from_snapshot}-{to_snapshot}"
        added_rows = self._rows_for_hashes(contract, to_snapshot, added)
        removed_rows = self._rows_for_hashes(contract, from_snapshot, removed)
        manifest = {
            "manifest_version": 1,
            "delivery_id": delivery_id,
            "contract_id": contract.id,
            "contract_hash": ch,
            "kind": "delta",
            "generation": gen,
            "from_view": before_fp,
            "to_view": target_fp,
            "from_snapshot": from_snapshot,
            "to_snapshot": to_snapshot,
            "added_hashes": dict(sorted(added.items())),
            "removed_hashes": dict(sorted(removed.items())),
            "counts": {"added": sum(added.values()), "removed": sum(removed.values())},
            "created_at": now_iso(),
        }
        return self._persist_package(
            manifest,
            {"added": contract.rows_to_arrow(added_rows) if added_rows else None,
             "removed": contract.rows_to_arrow(removed_rows) if removed_rows else None},
        )

    def offer_rebase(self, contract: ShareContract, snapshot_id: str) -> Package:
        """完整替换 = 新基线代际。构建期间接收仓仍查上一版，核验开放后才切换。"""
        ch = contract.identity_hash()
        new_gen = self._generation(ch) + 1
        view = self.build_view(contract, snapshot_id)
        rows = self._rows_for_hashes(contract, snapshot_id, view["multiset"])
        delivery_id = f"{ch[:12]}-g{new_gen}-rebase-{snapshot_id}"
        manifest = {
            "manifest_version": 1,
            "delivery_id": delivery_id,
            "contract_id": contract.id,
            "contract_hash": ch,
            "kind": "rebase",
            "generation": new_gen,
            "from_view": None,
            "to_view": view["view_fp"],
            "from_snapshot": None,
            "to_snapshot": snapshot_id,
            "multiset": dict(sorted(view["multiset"].items())),
            "counts": {"added": sum(view["multiset"].values()), "removed": 0},
            "created_at": now_iso(),
        }
        pkg = self._persist_package(manifest, {"full": contract.rows_to_arrow(rows)})
        self.con.execute(
            "UPDATE generations SET generation=? WHERE contract_hash=?", [new_gen, ch]
        )
        self.con.commit()
        return pkg

    # ---------- 交接、重发与确认 ----------
    def send(self, delivery_id: str, handoff_dir) -> Package:
        """把已持久化的传递包放入本地交接目录；重复调用结果字节一致。"""
        row = self.con.execute(
            "SELECT package_path FROM deliveries WHERE delivery_id=?", [delivery_id]
        ).fetchone()
        if row is None:
            raise ValueError(f"传递 {delivery_id} 不存在，无法发送")
        packages_dir = Path(handoff_dir) / "packages"
        return copy_package(Path(row[0]), packages_dir)

    def resend(self, delivery_id: str, handoff_dir) -> Package:
        """断线重送：同一传递身份、同一内容再放一次，不生成新一轮。"""
        return self.send(delivery_id, handoff_dir)

    def collect_ack(self, handoff_dir, delivery_id: str) -> dict[str, Any]:
        ack_path = Path(handoff_dir) / "acks" / f"{delivery_id}.json"
        if not ack_path.is_file():
            return {"delivery_id": delivery_id, "acked": False, "reason": "确认尚未抵达"}
        ack = json.loads(ack_path.read_text(encoding="utf-8"))
        row = self.con.execute(
            "SELECT content_hash, acked FROM deliveries WHERE delivery_id=?",
            [delivery_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"账本中没有传递 {delivery_id}")
        if ack["content_hash"] != row[0]:
            raise ValueError("确认的内容指纹与账本不符，拒绝采信")
        if not row[1]:
            self.con.execute(
                "UPDATE deliveries SET acked=1, acked_at=? WHERE delivery_id=?",
                [now_iso(), delivery_id],
            )
            self.con.commit()
        return {"delivery_id": delivery_id, "acked": True}

    def ledger(self) -> list[dict[str, Any]]:
        cols = [d[0] for d in self.con.execute(
            "SELECT delivery_id, kind, generation, from_view, to_view, content_hash, "
            "acked, acked_at FROM deliveries ORDER BY created_at").description]
        return [dict(zip(cols, r)) for r in self.con.execute(
            "SELECT delivery_id, kind, generation, from_view, to_view, content_hash, "
            "acked, acked_at FROM deliveries ORDER BY created_at").fetchall()]

    def status(self) -> dict[str, Any]:
        contracts = [r[0] for r in self.con.execute(
            "SELECT contract_hash FROM contracts ORDER BY accepted_at").fetchall()]
        out = {}
        for ch in contracts:
            out[ch[:12]] = {
                "generation": self._generation(ch),
                "open_baselines": self.open_baselines(ch),
                "deliveries": len(self.con.execute(
                    "SELECT 1 FROM deliveries WHERE contract_hash=?", [ch]).fetchall()),
                "acked": len(self.con.execute(
                    "SELECT 1 FROM deliveries WHERE contract_hash=? AND acked=1",
                    [ch]).fetchall()),
            }
        return out

    def reconcile(self) -> dict[str, Any]:
        """提供方自查：视图 Parquet 指纹、账本包完整性、退休基线。"""
        from .hashing import row_hash
        problems = []
        for ch, sid, fp_db, ms_raw, retired in self.con.execute(
                "SELECT contract_hash, snapshot_id, view_fp, multiset_json, retired "
                "FROM views").fetchall():
            contract = self.get_contract(ch)
            path = self.root / "views" / ch[:16] / f"{sid}.parquet"
            if not path.is_file():
                problems.append(f"视图 Parquet 缺失: {sid}")
                continue
            rows = contract.project_files(self.con, [path])
            ms = multiset_hashes(contract.columns, contract.types, rows)
            if view_fingerprint(ms) != fp_db or ms != json.loads(ms_raw):
                problems.append(f"视图 {sid} 重算指纹与账本不一致")
        for did, chash, pkg_path in self.con.execute(
                "SELECT delivery_id, content_hash, package_path FROM deliveries").fetchall():
            try:
                pkg = read_package(Path(pkg_path))
                from .store import manifest_content_hash
                if manifest_content_hash(pkg.manifest) != chash:
                    problems.append(f"传递 {did} 包内容指纹漂移")
            except Exception as exc:  # noqa: BLE001
                problems.append(f"传递 {did} 包不可读: {exc}")
        return {"ok": not problems, "problems": problems,
                "retired": [r[0] for r in self.con.execute(
                    "SELECT snapshot_id FROM views WHERE retired=1").fetchall()]}
