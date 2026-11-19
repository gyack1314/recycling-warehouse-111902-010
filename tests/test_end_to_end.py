"""端到端验收：两个独立持久化仓 + 本地文件交接，覆盖题述全部场景。

每个用例使用独立临时目录；所有“已发送/已接收”结论都从磁盘 DuckDB 与
Parquet/确认文件重新打开读取，不依赖进程内变量。
"""
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import duckdb

from market.contract import ShareContract
from market.errors import (
    ContentConflict,
    ContractMismatch,
    MissingBaseline,
    StaleGeneration,
    VerificationFailed,
)
from market.handoff import Handoff
from market.provider import ProviderStore
from market.receiver import ReceiverStore
from market.store import manifest_content_hash, write_parquet
from market.hashing import sha256_file

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "inputs" / "share-contract.json"
VERSIONS = ROOT / "inputs" / "versions.json"
CH_PREFIX = "f76696f7b4bd"


class EndToEndBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="market-e2e-"))
        # 输入复制到临时目录，与产物隔离；原件保持只读语义
        self.inputs = self.tmp / "inputs"
        shutil.copytree(ROOT / "inputs", self.inputs)
        self.provider_dir = self.tmp / "provider_store"
        self.receiver_dir = self.tmp / "receiver_store"
        self.handoff_dir = self.tmp / "handoff"
        self.contract = ShareContract.from_file(CONTRACT_PATH)
        self.application = {"application_id": "app-test", "applicant": "county",
                            "contract": self.contract.to_dict()}
        self.handoff = Handoff(self.handoff_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def provider(self):
        return ProviderStore(self.provider_dir, self.inputs,
                             self.inputs / "versions.json")

    def receiver(self):
        return ReceiverStore(self.receiver_dir)

    def bootstrap(self, snapshot="S2"):
        provider = self.provider()
        receiver = self.receiver()
        provider.accept_application(self.application)
        receiver.adopt_contract(self.application)
        pkg = provider.offer_snapshot(self.contract, snapshot)
        provider.send(pkg.delivery_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(pkg.delivery_id),
                         handoff_dir=self.handoff_dir)
        provider.collect_ack(self.handoff_dir, pkg.delivery_id)
        return provider, receiver, pkg.delivery_id

    def fresh_receiver(self):
        """模拟全新进程：重新打开磁盘上的县仓。"""
        return ReceiverStore(self.receiver_dir)

    def fresh_provider(self):
        return ProviderStore(self.provider_dir, self.inputs,
                             self.inputs / "versions.json")

    def current_rows(self):
        r = self.fresh_receiver()
        rows = r.query_current()
        r.close()
        return rows


class HappyPathAndResend(EndToEndBase):
    def test_snapshot_delta_and_persistent_state(self):
        provider, receiver, snap_id = self.bootstrap()
        self.assertEqual(len(self.current_rows()), 3)
        # 新进程读 DuckDB 账本，状态仍在
        ledger = self.fresh_provider().ledger()
        self.assertTrue(all(d["acked"] for d in ledger))

        delta = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(delta.delivery_id, self.handoff_dir)
        result = receiver.receive(self.handoff.package_dir(delta.delivery_id),
                                  handoff_dir=self.handoff_dir)
        self.assertEqual(result["added"], 0)
        self.assertEqual(result["removed"], 2)  # 一份 paper 撞行 + glass 退出
        rows = self.current_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["material"], "paper")
        provider.close(); receiver.close()

    def test_lost_ack_resend_does_not_add_round(self):
        provider, receiver, snap_id = self.bootstrap()
        delta = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(delta.delivery_id, self.handoff_dir)
        # 首次接收在写确认前断线
        with self.assertRaises(RuntimeError):
            receiver.receive(self.handoff.package_dir(delta.delivery_id),
                             handoff_dir=self.handoff_dir, crash_hook="before_ack")
        self.assertFalse(self.handoff.ack_path(delta.delivery_id).exists())
        # 数据已持久化
        self.assertEqual(len(self.current_rows()), 1)
        # 提供方确认丢失 -> 重发同一身份同内容
        provider.resend(delta.delivery_id, self.handoff_dir)
        again = self.fresh_receiver().receive(
            self.handoff.package_dir(delta.delivery_id), handoff_dir=self.handoff_dir)
        self.assertEqual(again["status"], "duplicate_ignored")
        # 确认补齐，且没有新增轮次、没有重复增减
        self.assertEqual(provider.collect_ack(self.handoff_dir,
                                              delta.delivery_id)["acked"], True)
        rounds = self.fresh_receiver().rounds()
        self.assertEqual(len(rounds), 2)
        self.assertEqual(len(self.current_rows()), 1)
        provider.close(); receiver.close()

    def test_crash_before_publish_leaves_warehouse_untouched(self):
        provider, receiver, snap_id = self.bootstrap()
        delta = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(delta.delivery_id, self.handoff_dir)
        with self.assertRaises(RuntimeError):
            receiver.receive(self.handoff.package_dir(delta.delivery_id),
                             handoff_dir=self.handoff_dir,
                             crash_hook="before_publish")
        self.assertEqual(len(self.current_rows()), 3)  # 仍是上一版
        # 恢复后正常接收
        ok = self.fresh_receiver().receive(
            self.handoff.package_dir(delta.delivery_id), handoff_dir=self.handoff_dir)
        self.assertEqual(ok["status"], "applied")
        self.assertEqual(len(self.current_rows()), 1)
        provider.close(); receiver.close()


class MultisetAndRepack(EndToEndBase):
    def test_physical_repack_creates_no_round(self):
        provider = self.provider()
        provider.accept_application(self.application)
        v1 = provider.build_view(self.contract, "S1")
        v2 = provider.build_view(self.contract, "S2")
        self.assertEqual(v1["view_fp"], v2["view_fp"])
        self.assertEqual(sum(v1["multiset"].values()), 3)
        # 相同内容重复构建不产生新视图行
        v2b = provider.build_view(self.contract, "S2")
        self.assertTrue(v2b["rebuilt"])
        n_views = provider.con.execute(
            "SELECT count(*) FROM views WHERE snapshot_id='S2'").fetchone()[0]
        self.assertEqual(n_views, 1)
        provider.close()

    def test_duplicate_copies_survive_full_path(self):
        # S2 视图含两份完全相同的 paper 行（无主键、不同 private_ticket）
        provider, receiver, _ = self.bootstrap()
        rows = self.current_rows()
        paper = [x for x in rows if x["material"] == "paper"]
        self.assertEqual(len(paper), 2)  # 份数保留
        # DuckDB 直接查 Parquet 复核
        con = duckdb.connect()
        n = con.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(self.receiver_dir / "current.parquet")]).fetchone()[0]
        self.assertEqual(n, 3)
        con.close(); provider.close(); receiver.close()

    def test_projection_collision_and_filter_exit_are_net_changes(self):
        provider, receiver, _ = self.bootstrap()
        delta = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(delta.delivery_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(delta.delivery_id),
                         handoff_dir=self.handoff_dir)
        manifest = json.loads((self.handoff.package_dir(delta.delivery_id)
                               / "manifest.json").read_text(encoding="utf-8"))
        # paper 2->1 净减 1；glass 因 district 更正为 C 退出筛选，净减 1
        self.assertEqual(manifest["counts"], {"added": 0, "removed": 2})
        self.assertEqual(len(manifest["removed_hashes"]), 2)
        provider.close(); receiver.close()

    def test_round_baseline_is_queryable(self):
        provider, receiver, _ = self.bootstrap()
        delta = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(delta.delivery_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(delta.delivery_id),
                         handoff_dir=self.handoff_dir)
        r = self.fresh_receiver()
        rounds = r.rounds()
        self.assertIsNone(rounds[0]["from_view"])
        self.assertEqual(rounds[1]["from_view"], rounds[0]["to_view"])
        # 第 1 轮留档仍是 3 行，当前是 1 行：轮次针对哪个基线可查
        self.assertEqual(len(r.query_round(1)), 3)
        self.assertEqual(len(r.query_round(2)), 1)
        r.close(); provider.close(); receiver.close()


class ConflictAndTamper(EndToEndBase):
    def _forge_same_id_different_content(self, delivery_id):
        pkg_dir = self.handoff.package_dir(delivery_id)
        evil = self.contract.rows_to_arrow([
            {"day": "2099-01-01", "district": "A", "material": "hack",
             "weight": "9.999"}])
        m = json.loads((pkg_dir / "manifest.json").read_text(encoding="utf-8"))
        rel = m["payloads"]["full"]["file"]
        write_parquet(evil, pkg_dir / rel)
        m["payloads"]["full"]["sha256"] = sha256_file(pkg_dir / rel)
        m["payloads"]["full"]["rows"] = 1
        m["content_hash"] = manifest_content_hash(m)
        (pkg_dir / "manifest.json").write_text(
            json.dumps(m, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8")

    def test_same_id_different_content_stops_and_keeps_conflict(self):
        provider, receiver, snap_id = self.bootstrap()
        before = self.current_rows()
        self._forge_same_id_different_content(snap_id)
        with self.assertRaises(ContentConflict):
            receiver.receive(self.handoff.package_dir(snap_id),
                             handoff_dir=self.handoff_dir)
        conflicts = self.fresh_receiver().conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["reason"], "same_id_different_content")
        self.assertEqual(self.current_rows(), before)  # 未被后到者覆盖
        provider.close(); receiver.close()

    def test_corrupt_payload_is_rejected(self):
        provider, receiver, snap_id = self.bootstrap()
        payload = self.handoff.package_dir(snap_id) / "payloads" / "full.parquet"
        payload.write_bytes(payload.read_bytes() + b"\x00")
        with self.assertRaises(VerificationFailed):
            receiver.receive(self.handoff.package_dir(snap_id),
                             handoff_dir=self.handoff_dir)
        provider.close(); receiver.close()

    def test_unknown_contract_delivery_refused(self):
        provider = self.provider()
        provider.accept_application(self.application)
        pkg = provider.offer_snapshot(self.contract, "S2")
        provider.send(pkg.delivery_id, self.handoff_dir)
        # 一个从未接纳该契约的新县仓
        other = ReceiverStore(self.tmp / "other_receiver")
        with self.assertRaises(ContractMismatch):
            other.receive(self.handoff.package_dir(pkg.delivery_id),
                          handoff_dir=self.handoff_dir)
        other.close(); provider.close()


class GenerationAndReplacement(EndToEndBase):
    def test_retire_blocks_delta_and_rebase_cuts_generations(self):
        provider, receiver, _ = self.bootstrap()
        provider.offer_delta(self.contract, "S2", "S3")  # 构建 S3 视图
        ch = self.contract.identity_hash()
        provider.retire(ch, ["S1", "S2"])
        with self.assertRaises(MissingBaseline):
            provider.offer_delta(self.contract, "S2", "S4")
        rebase = provider.offer_rebase(self.contract, "S4")
        self.assertEqual(rebase.manifest["generation"], 2)
        provider.send(rebase.delivery_id, self.handoff_dir)
        # 替换构建期间县仓仍查上一版
        self.assertEqual(len(self.current_rows()), 3)
        result = receiver.receive(self.handoff.package_dir(rebase.delivery_id),
                                  handoff_dir=self.handoff_dir)
        self.assertEqual(result["generation"], 2)
        rows = self.current_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["material"], "metal")
        provider.close(); receiver.close()

    def test_old_generation_delta_rejected_after_rebase(self):
        provider, receiver, _ = self.bootstrap()
        # g1 时期囤一个 S3->S4 差量但不投递（先构建两端视图）
        provider.build_view(self.contract, "S3")
        provider.build_view(self.contract, "S4")
        hoarded = provider.offer_delta(self.contract, "S3", "S4")
        d_s23 = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(d_s23.delivery_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(d_s23.delivery_id),
                         handoff_dir=self.handoff_dir)
        provider.offer_rebase(self.contract, "S4")
        reb_id = f"{CH_PREFIX}-g2-rebase-S4"
        provider.send(reb_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(reb_id),
                         handoff_dir=self.handoff_dir)
        # 新基线建立后旧代际差量到达 -> 拒绝并留痕
        provider.send(hoarded.delivery_id, self.handoff_dir)
        with self.assertRaises(StaleGeneration):
            self.fresh_receiver().receive(
                self.handoff.package_dir(hoarded.delivery_id),
                handoff_dir=self.handoff_dir)
        refused = receiver.con.execute(
            "SELECT reason FROM refused WHERE delivery_id=?",
            [hoarded.delivery_id]).fetchone()
        self.assertEqual(refused[0], "stale_generation")
        provider.close(); receiver.close()

    def test_missing_baseline_delta_refused(self):
        provider, receiver, _ = self.bootstrap()
        # 直接投递一个基线不匹配的差量（县仓在 S2，差量声称从别的视图出发）
        provider.build_view(self.contract, "S3")
        provider.build_view(self.contract, "S4")
        delta = provider.offer_delta(self.contract, "S3", "S4")
        provider.send(delta.delivery_id, self.handoff_dir)
        with self.assertRaises(MissingBaseline):
            receiver.receive(self.handoff.package_dir(delta.delivery_id),
                             handoff_dir=self.handoff_dir)
        provider.close(); receiver.close()


class ContractChange(EndToEndBase):
    def test_changed_filter_is_new_contract_old_delivery_unusable(self):
        provider, receiver, snap_id = self.bootstrap()
        # 新契约：筛选放宽到 C —— 身份哈希不同
        new_contract = ShareContract.from_dict({
            **self.contract.to_dict(),
            "id": "share-v2",
            "where": {"district_in": ["A", "B", "C"]},
        })
        self.assertNotEqual(new_contract.identity_hash(),
                            self.contract.identity_hash())
        # 未明确接纳前，新契约传递不能入仓
        app2 = {"application_id": "app-v2", "applicant": "county",
                "contract": new_contract.to_dict()}
        provider.accept_application(app2)
        pkg2 = provider.offer_snapshot(new_contract, "S3")  # 含 district C 的 glass
        provider.send(pkg2.delivery_id, self.handoff_dir)
        with self.assertRaises(ContractMismatch):
            self.fresh_receiver().receive(
                self.handoff.package_dir(pkg2.delivery_id),
                handoff_dir=self.handoff_dir)
        # 明确接纳后：新契约是独立身份，旧传递不能用在新条件上
        receiver.adopt_contract(app2)
        result = receiver.receive(self.handoff.package_dir(pkg2.delivery_id),
                                  handoff_dir=self.handoff_dir)
        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["generation"], 1)  # 新契约独立从第 1 代起线
        materials = sorted(x["material"] for x in self.current_rows())
        self.assertEqual(materials, ["glass", "paper"])  # C 区 glass 进入新契约视图
        # 原契约线状态不受影响
        original = receiver._state(self.contract.identity_hash())
        self.assertEqual(original[1], 1)
        provider.close(); receiver.close()


class Reconcile(EndToEndBase):
    def test_both_sides_reconcile_clean_and_replay(self):
        provider, receiver, _ = self.bootstrap()
        d = provider.offer_delta(self.contract, "S2", "S3")
        provider.send(d.delivery_id, self.handoff_dir)
        receiver.receive(self.handoff.package_dir(d.delivery_id),
                         handoff_dir=self.handoff_dir)
        rep = provider.reconcile()
        self.assertTrue(rep["ok"], rep["problems"])
        rec = self.fresh_receiver().reconcile()
        self.assertTrue(rec["ok"], rec["problems"])
        self.assertEqual(rec["rounds"], 2)
        provider.close(); receiver.close()


if __name__ == "__main__":
    unittest.main()
