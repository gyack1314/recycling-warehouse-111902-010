"""接收方县仓：核验、原子入仓、冲突留痕、对账与查询。

入仓保证：
- 同一传递身份重收且内容一致 -> 幂等无操作；内容不同 -> 停止并留冲突，绝不覆盖；
- 同一轮的删除与增加在一次发布中完成，查询永远看不到“只删未增”的中间状态；
- 旧代际差量在新基线建立后拒绝；基线对不上要求补传/完整重建；
- 全部进度落 DuckDB，数据落真实 Parquet；进程重启后状态仍在。
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from .contract import ShareContract
from .errors import (
    ContentConflict,
    ContractMismatch,
    MissingBaseline,
    StaleGeneration,
    VerificationFailed,
)
from .hashing import (
    apply_delta,
    multiset_hashes,
    row_hash,
    view_fingerprint,
)
from .store import (
    Package,
    RECEIVER_DDL,
    atomic_write_text,
    now_iso,
    open_db,
    read_package,
    verify_package_static,
    write_parquet,
)


class ReceiverStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        for sub in ("packages", "staging", "rounds", "current_by_contract"):
            (self.root / sub).mkdir(exist_ok=True)
        self.db_path = self.root / "receiver.duckdb"
        self.current_path = self.root / "current.parquet"
        # 按契约隔离的当前版本；current.parquet 是最近开放版本的原子副本
        self.latest_dir = self.root / "current_by_contract"
        self.con = open_db(self.db_path)
        self.con.execute(RECEIVER_DDL)
        self._refresh_view()
        self.recover()

    def close(self):
        self.con.close()

    # ---------- 契约接纳 ----------
    def adopt_contract(self, application: dict[str, Any]) -> str:
        """明确接纳共享申请。筛选/列/类型变化产生新契约身份，须重新接纳。"""
        contract = ShareContract.from_dict(application["contract"])
        ch = contract.identity_hash()
        existing = self.con.execute(
            "SELECT 1 FROM accepted_contracts WHERE contract_hash=?", [ch]
        ).fetchone()
        if existing is None:
            self.con.execute(
                "INSERT INTO accepted_contracts VALUES (?, ?, ?)",
                (ch, json.dumps(contract.to_dict(), ensure_ascii=False, sort_keys=True),
                 now_iso()),
            )
            self.con.commit()
        return ch

    def active_contract(self) -> ShareContract | None:
        row = self.con.execute(
            "SELECT contract_json FROM accepted_contracts ORDER BY accepted_at DESC LIMIT 1"
        ).fetchone()
        return None if row is None else ShareContract.from_dict(json.loads(row[0]))

    def _require_contract(self, contract_hash: str) -> ShareContract:
        row = self.con.execute(
            "SELECT contract_json FROM accepted_contracts WHERE contract_hash=?",
            [contract_hash],
        ).fetchone()
        if row is None:
            raise ContractMismatch(
                f"接收仓未接纳契约 {contract_hash[:12]}…，该传递不能入仓"
            )
        return ShareContract.from_dict(json.loads(row[0]))

    # ---------- 当前状态（按契约隔离，可并存多条契约线）----------
    def _state(self, contract_hash: str | None = None):
        if contract_hash is None:
            return self.con.execute(
                "SELECT contract_hash, generation, view_fp, multiset_json, "
                "baseline_snapshot, updated_at FROM current_state "
                "ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        return self.con.execute(
            "SELECT contract_hash, generation, view_fp, multiset_json, "
            "baseline_snapshot, updated_at FROM current_state "
            "WHERE contract_hash=?", [contract_hash]
        ).fetchone()

    def _refresh_view(self):
        if self.current_path.is_file():
            # CREATE VIEW 不支持 prepared 参数；路径由本仓生成，转义单引号后内联
            safe = str(self.current_path).replace("'", "''")
            self.con.execute(
                "CREATE OR REPLACE VIEW current_view AS "
                f"SELECT * FROM read_parquet('{safe}')"
            )

    def recover(self) -> dict[str, Any]:
        """启动恢复：清理未完成的暂存区。

        发布顺序是先原子替换 current.parquet、再写账本；因此最坏情况是
        “数据已换、账本未写”，重收同一传递时会再次核验并补记账，天然幂等，
        不会出现只删未增或重复增减。
        """
        cleaned = []
        staging = self.root / "staging"
        if staging.exists():
            for child in staging.iterdir():
                cleaned.append(child.name)
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        self._refresh_view()
        return {"cleaned_staging": cleaned}

    # ---------- 核验辅助 ----------
    def _payload_rows(self, contract: ShareContract, pkg: Package, name: str):
        path = pkg.payload_path(name)
        if path is None:
            return []
        return contract.project_files(self.con, [path])

    def _hash_rows(self, contract: ShareContract, rows) -> dict[str, int]:
        return multiset_hashes(contract.columns, contract.types, rows)

    def _record_conflict(self, delivery_id: str, reason: str, seen: str, stored: str,
                         detail: str):
        self.con.execute(
            "INSERT INTO conflicts(conflict_id, delivery_id, reason, "
            "seen_content_hash, stored_content_hash, detail, created_at) "
            "VALUES (nextval('conflict_seq'), ?, ?, ?, ?, ?, ?)",
            (delivery_id, reason, seen, stored, detail, now_iso()),
        )
        self.con.commit()

    def _refuse(self, delivery_id: str, reason: str, detail: str):
        self.con.execute(
            "INSERT OR REPLACE INTO refused VALUES (?, ?, ?, ?)",
            (delivery_id, reason, detail, now_iso()),
        )
        self.con.commit()
    # ---------- 接收主流程 ----------
    def receive(self, package_dir, handoff_dir=None, crash_hook: str | None = None) -> dict[str, Any]:
        """核验并原子接收一个传递包。

        crash_hook:
          'before_publish' — 暂存核验后、发布前崩溃（包已到，仓未变）；
          'before_ack'     — 入仓已提交、确认写出前崩溃（模拟确认丢失）。
        """
        pkg = read_package(Path(package_dir))
        verify_package_static(pkg)  # 清单指纹 + 载荷摘要 + 行数
        m = pkg.manifest
        contract = self._require_contract(m["contract_hash"])

        # 1) 幂等 / 同身份冲突：以持久化账本裁决，不看进程变量、不看文件名
        ledger_row = self.con.execute(
            "SELECT content_hash, applied_at FROM ledger WHERE delivery_id=?",
            [pkg.delivery_id],
        ).fetchone()
        if ledger_row is not None:
            if ledger_row[0] == pkg.content_hash:
                # 幂等重收：数据绝不重复增减；但若上次在写确认前断线，这里补齐确认
                if handoff_dir is not None:
                    ack_path = Path(handoff_dir) / "acks" / f"{pkg.delivery_id}.json"
                    if not ack_path.is_file():
                        ack_dir = Path(handoff_dir) / "acks"
                        ack_dir.mkdir(parents=True, exist_ok=True)
                        atomic_write_text(
                            ack_path,
                            json.dumps({"delivery_id": pkg.delivery_id,
                                        "content_hash": pkg.content_hash,
                                        "applied_at": ledger_row[1]},
                                       ensure_ascii=False, indent=2, sort_keys=True),
                        )
                return {"delivery_id": pkg.delivery_id, "status": "duplicate_ignored",
                        "note": "同一传递身份同内容重收，不重复增减；确认已确保落盘"}
            self._record_conflict(
                pkg.delivery_id, "same_id_different_content",
                pkg.content_hash, ledger_row[0],
                "同一传递身份抵达了不同内容，已停止；不以后到者覆盖",
            )
            raise ContentConflict(
                f"传递 {pkg.delivery_id} 身份相同但内容不同，冲突已留痕，入仓中止"
            )

        state = self._state(m["contract_hash"])
        current_gen = 0 if state is None else int(state[1])
        kind = m["kind"]

        # 2) 代际与基线守卫
        if kind == "snapshot":
            if state is not None and state[0] == m["contract_hash"]:
                self._refuse(pkg.delivery_id, "stale_generation",
                             f"同契约仓内已有第 {current_gen} 代基线，不接受重复初始快照")
                raise StaleGeneration("仓内已有基线，初始快照不再适用")
            if int(m["generation"]) != 1:
                raise StaleGeneration("初始快照必须是第 1 代")
        elif kind == "rebase":
            # 代际只在同一契约身份内比较；不同契约各自从第 1 代独立起线
            same_contract_gen = int(state[1]) if state is not None and \
                state[0] == m["contract_hash"] else 0
            if int(m["generation"]) <= same_contract_gen:
                self._refuse(pkg.delivery_id, "stale_generation",
                             f"完整替换代际 {m['generation']} 不新于该契约当前 "
                             f"{same_contract_gen}")
                raise StaleGeneration("旧代际的完整替换不能在新基线之后接入")
        elif kind == "delta":
            if state is None:
                raise MissingBaseline("空仓不能接差量，需要完整基线")
            if int(m["generation"]) != current_gen:
                self._refuse(pkg.delivery_id, "stale_generation",
                             f"差量代际 {m['generation']} 与当前 {current_gen} 不一致；"
                             "新基线建立后旧代际差量不得接入")
                raise StaleGeneration("旧代际差量被拒绝")
            if m["from_view"] != state[2]:
                self._refuse(pkg.delivery_id, "missing_baseline",
                             f"差量基线 {str(m['from_view'])[:12]} 与仓内 "
                             f"{str(state[2])[:12]} 不一致，需补传或完整重建")
                raise MissingBaseline("差量基线与县仓当前基线不一致")
        else:
            raise VerificationFailed(f"未知传递类型 {kind}")

        # 3) 按类型独立核验内容，并得到发布后的目标多重集
        if kind in ("snapshot", "rebase"):
            full_rows = self._payload_rows(contract, pkg, "full")
            target_ms = self._hash_rows(contract, full_rows)
            if view_fingerprint(target_ms) != m["to_view"]:
                raise VerificationFailed("全量载荷重算指纹与清单 to_view 不符")
            if m.get("multiset") and dict(sorted(target_ms.items())) != \
                    dict(sorted(m["multiset"].items())):
                raise VerificationFailed("全量载荷重数与清单多重集不符")
            target_rows = full_rows
            added_n = sum(target_ms.values()) if kind == "snapshot" else None
            removed_n = 0 if kind == "snapshot" else None
        else:
            added_rows = self._payload_rows(contract, pkg, "added")
            removed_rows = self._payload_rows(contract, pkg, "removed")
            added_ms = self._hash_rows(contract, added_rows)
            removed_ms = self._hash_rows(contract, removed_rows)
            if added_ms != dict(m["added_hashes"]):
                raise VerificationFailed("added 载荷行哈希/重数与清单不符（重复份数丢失？）")
            if removed_ms != dict(m["removed_hashes"]):
                raise VerificationFailed("removed 载荷行哈希/重数与清单不符")
            base_ms = json.loads(state[3])
            target_ms = apply_delta(base_ms, added_ms, removed_ms)  # 负数即抛错
            if view_fingerprint(target_ms) != m["to_view"]:
                raise VerificationFailed("差量应用后指纹与清单 to_view 不符")
            example: dict[str, dict] = {}
            for row in self._payload_rows(contract, pkg, "added"):
                example.setdefault(row_hash(contract.columns, contract.types, row), row)
            for row in contract.project_files(self.con, [self.current_path]):
                example.setdefault(row_hash(contract.columns, contract.types, row), row)
            target_rows = []
            for rh, n in target_ms.items():
                if rh not in example:
                    raise VerificationFailed(f"目标多重集缺少行内容 {rh[:12]}…")
                target_rows.extend([dict(example[rh]) for _ in range(n)])
            added_n = sum(added_ms.values())
            removed_n = sum(removed_ms.values())

        # 4) 暂存并回读复核（此时对外查询仍指向上一版）
        staging = self.root / "staging" / pkg.delivery_id
        staging.mkdir(parents=True, exist_ok=True)
        staging_file = staging / "current.parquet"
        write_parquet(contract.rows_to_arrow(target_rows), staging_file)
        reread = contract.project_files(self.con, [staging_file])
        if view_fingerprint(self._hash_rows(contract, reread)) != m["to_view"]:
            raise VerificationFailed("暂存 Parquet 回读指纹不符，发布中止")

        if crash_hook == "before_publish":
            shutil.rmtree(staging)
            raise RuntimeError("注入断点 before_publish：暂存核验完成后崩溃，仓内数据未变化")

        # 5) 原子发布：先落契约专属版本，再一次 rename 切换公开指针。
        #    全程不存在“只删未增”的中间状态；其他契约的开放版本不受影响。
        contract_current = self.latest_dir / f"{m['contract_hash'][:16]}.parquet"
        os.replace(staging_file, contract_current)
        shutil.rmtree(staging, ignore_errors=True)
        public_tmp = self.current_path.with_suffix(".parquet.pubtmp")
        try:
            os.link(contract_current, public_tmp)  # 同盘硬链接，零拷贝
        except OSError:
            shutil.copy2(contract_current, public_tmp)
        os.replace(public_tmp, self.current_path)
        self._refresh_view()

        # 6) 账本、轮次、当前状态在单个事务落盘
        applied_at = now_iso()
        self.con.execute("BEGIN TRANSACTION")
        self.con.execute(
            "INSERT INTO ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (pkg.delivery_id, m["contract_hash"], kind, int(m["generation"]),
             m.get("from_view"), m["to_view"], pkg.content_hash, applied_at),
        )
        round_id = self.con.execute("SELECT nextval('round_seq')").fetchone()[0]
        round_file = self.root / "rounds" / f"round_{round_id:06d}.parquet"
        shutil.copy2(self.current_path, round_file)
        self.con.execute(
            "INSERT INTO rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (round_id, pkg.delivery_id, kind, int(m["generation"]),
             m.get("from_view"), m["to_view"],
             added_n if added_n is not None else -1,
             removed_n if removed_n is not None else -1, applied_at),
        )
        self.con.execute(
            "INSERT OR REPLACE INTO current_state VALUES (?, ?, ?, ?, ?, ?)",
            (m["contract_hash"], int(m["generation"]), m["to_view"],
             json.dumps(target_ms, sort_keys=True), m.get("to_snapshot"), applied_at),
        )
        self.con.execute("COMMIT")

        # 7) 保留接收包原件用于回查（须在 ack 断点之前，保证崩溃路径也有留档）
        keep = self.root / "packages" / pkg.delivery_id
        if not keep.exists():
            shutil.copytree(pkg.root, keep)

        # 8) 落确认（持久化文件，不靠进程变量）。此点之前崩溃即“确认丢失”场景
        if crash_hook == "before_ack":
            raise RuntimeError("注入断点 before_ack：入仓已提交但确认未发出")
        if handoff_dir is not None:
            ack_dir = Path(handoff_dir) / "acks"
            ack_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_text(
                ack_dir / f"{pkg.delivery_id}.json",
                json.dumps({"delivery_id": pkg.delivery_id,
                            "content_hash": pkg.content_hash,
                            "applied_at": applied_at},
                           ensure_ascii=False, indent=2, sort_keys=True),
            )

        return {"delivery_id": pkg.delivery_id, "status": "applied",
                "kind": kind, "generation": int(m["generation"]),
                "round_id": round_id, "added": added_n, "removed": removed_n,
                "to_view": m["to_view"]}

    # ---------- 查询 ----------
    def query_current(self, sql: str | None = None) -> list[dict[str, Any]]:
        """对当前开放版本执行 DuckDB 查询（替换构建期间仍只看到上一版）。"""
        if not self.current_path.is_file():
            return []
        self._refresh_view()
        sql = sql or "SELECT * FROM current_view ORDER BY 1, 2, 3, 4"
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def query_round(self, round_id: int, sql: str | None = None) -> list[dict[str, Any]]:
        path = self.root / "rounds" / f"round_{round_id:06d}.parquet"
        if not path.is_file():
            raise MissingBaseline(f"轮次 {round_id} 的 Parquet 不存在")
        sql = sql or "SELECT * FROM data ORDER BY 1, 2, 3, 4"
        safe = str(path).replace("'", "''")
        self.con.execute(
            "CREATE OR REPLACE TEMP VIEW data AS "
            f"SELECT * FROM read_parquet('{safe}')"
        )
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def rounds(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            "SELECT round_id, delivery_id, kind, generation, from_view, to_view, "
            "added_rows, removed_rows, applied_at FROM rounds ORDER BY round_id"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def conflicts(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            "SELECT conflict_id, delivery_id, reason, seen_content_hash, "
            "stored_content_hash, detail, created_at FROM conflicts ORDER BY conflict_id"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def state(self) -> dict[str, Any] | None:
        s = self._state()
        if s is None:
            return None
        return {"contract_hash": s[0], "generation": s[1], "view_fp": s[2],
                "distinct_rows": len(json.loads(s[3])),
                "total_copies": sum(json.loads(s[3]).values()),
                "baseline_snapshot": s[4], "updated_at": s[5]}

    # ---------- 对账 ----------
    def reconcile(self) -> dict[str, Any]:
        """县仓对账：账本/包/轮次/当前 Parquet/重放链路全部交叉核对。"""
        problems: list[str] = []
        contract = self.active_contract()
        if contract is None:
            return {"ok": False, "problems": ["尚未接纳任何共享契约"]}

        # 当前 Parquet 指纹必须等于 current_state
        st = self._state()
        if st is None:
            problems.append("账本无当前状态")
        elif self.current_path.is_file():
            rows = contract.project_files(self.con, [self.current_path])
            ms = self._hash_rows(contract, rows)
            if view_fingerprint(ms) != st[2]:
                problems.append("current.parquet 指纹与 current_state 不符")
            if ms != json.loads(st[3]):
                problems.append("current.parquet 重数与 current_state 不符")
        else:
            problems.append("current.parquet 缺失")

        # 每一轮的留档 Parquet 指纹必须等于该轮清单 to_view
        for rid, did, to_view in self.con.execute(
                "SELECT round_id, delivery_id, to_view FROM rounds ORDER BY round_id"
        ).fetchall():
            rp = self.root / "rounds" / f"round_{rid:06d}.parquet"
            if not rp.is_file():
                problems.append(f"轮次 {rid} 留档缺失")
                continue
            ms = self._hash_rows(contract, contract.project_files(self.con, [rp]))
            if view_fingerprint(ms) != to_view:
                problems.append(f"轮次 {rid} 留档指纹与清单不符")

        # 账本中的接收包必须仍可静态核验
        for did, chash in self.con.execute(
                "SELECT delivery_id, content_hash FROM ledger").fetchall():
            pkg_dir = self.root / "packages" / did
            if not pkg_dir.is_dir():
                problems.append(f"接收包 {did} 缺失")
                continue
            try:
                pkg = read_package(pkg_dir)
                from .store import manifest_content_hash
                verify_package_static(pkg)
                if manifest_content_hash(pkg.manifest) != chash:
                    problems.append(f"接收包 {did} 内容指纹漂移")
            except Exception as exc:  # noqa: BLE001
                problems.append(f"接收包 {did} 核验失败: {exc}")

        # 重放链路：从第一轮开始按账本顺序重放，终态须等于当前状态
        replay_ms: dict[str, int] = {}
        for did, kind, fv, tv in self.con.execute(
                "SELECT delivery_id, kind, from_view, to_view FROM ledger ORDER BY applied_at"
        ).fetchall():
            pkg = read_package(self.root / "packages" / did)
            if kind in ("snapshot", "rebase"):
                replay_ms = self._hash_rows(
                    contract, self._payload_rows(contract, pkg, "full"))
            else:
                add = self._hash_rows(
                    contract, self._payload_rows(contract, pkg, "added"))
                rem = self._hash_rows(
                    contract, self._payload_rows(contract, pkg, "removed"))
                replay_ms = apply_delta(replay_ms, add, rem)
            if view_fingerprint(replay_ms) != tv:
                problems.append(f"重放至 {did} 时指纹不符")
        if st is not None and view_fingerprint(replay_ms) != st[2]:
            problems.append("全链路重放终态与当前状态不一致")

        return {
            "ok": not problems,
            "problems": problems,
            "rounds": len(self.rounds()),
            "conflicts": len(self.conflicts()),
            "state": self.state(),
        }
