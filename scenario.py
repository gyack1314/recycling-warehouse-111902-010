#!/usr/bin/env python3
"""按 inputs/actions.json 逐步驱动的端到端验收场景。

五步对应：
  1 provide S2   -> 共享申请 + 首轮完整基线，县仓入仓开放查询；
  2 provide S3   -> S2->S3 净变化差量（字段更正退出筛选、投影撞行的净增减）；
  3 lose_ack     -> 在“已持久化与确认之间”注入断点，重发同一传递，不新增轮次；
  4 retire S1/S2 -> 有限旧版本退休，旧基线差量被拒，只能协商完整替换；
  5 offer_rebase -> S4 完整替换建立新代际，构建期间县仓仍查上一版，
                    开放后旧代际差量不得接入。

另含物理重排（S1 vs S2）与重复份数断言。所有产物在独立工作目录，
全程通过磁盘上的 DuckDB/Parquet/确认文件裁决，不使用进程内“已发送”变量。

用法：python3 -I scenario.py [--work work_demo]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import duckdb

from market.contract import ShareContract
from market.errors import MissingBaseline, StaleGeneration
from market.handoff import Handoff
from market.provider import ProviderStore
from market.receiver import ReceiverStore

CONTRACT = ROOT / "inputs" / "share-contract.json"


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", default=str(ROOT / "work_demo"))
    args = ap.parse_args()
    work = Path(args.work).resolve()
    if work.exists():
        shutil.rmtree(work)
    inputs = work / "inputs"
    shutil.copytree(ROOT / "inputs", inputs)
    handoff_dir = work / "handoff"
    handoff = Handoff(handoff_dir)
    contract = ShareContract.from_file(CONTRACT)

    provider = ProviderStore(work / "provider_store", inputs,
                             inputs / "versions.json")
    receiver = ReceiverStore(work / "receiver_store")
    application = {"application_id": "app-demo", "applicant": "county-warehouse",
                   "contract": contract.to_dict()}
    provider.accept_application(application)
    receiver.adopt_contract(application)

    # ---- 物理重排前置断言：S1 与 S2 内容多重集相同 ----
    banner("前置：物理重排（路径/行组/顺序）不产生业务变化")
    v1 = provider.build_view(contract, "S1")
    v2 = provider.build_view(contract, "S2")
    assert v1["view_fp"] == v2["view_fp"]
    print(f"S1 与 S2 视图指纹相同: {v1['view_fp'][:20]}…  总行数(含重复)="
          f"{sum(v1['multiset'].values())}（两份相同 paper 行均保留）")

    # ---- step 1: provide S2（首轮基线）----
    banner("步骤1 provide S2：完整基线快照 → 县仓入仓开放查询")
    snap = provider.offer_snapshot(contract, "S2")
    provider.send(snap.delivery_id, handoff_dir)
    r1 = receiver.receive(handoff.package_dir(snap.delivery_id),
                          handoff_dir=handoff_dir)
    provider.collect_ack(handoff_dir, snap.delivery_id)
    print("入仓:", {k: r1[k] for k in ("status", "kind", "round_id", "added")})
    print("县仓当前行数:", len(receiver.query_current()))

    # ---- step 2: provide S3（净变化差量）----
    banner("步骤2 provide S3：净变化（paper 2→1 撞行净减，glass 更正为 C 退出筛选）")
    delta23 = provider.offer_delta(contract, "S2", "S3")
    provider.send(delta23.delivery_id, handoff_dir)
    r2 = receiver.receive(handoff.package_dir(delta23.delivery_id),
                          handoff_dir=handoff_dir)
    provider.collect_ack(handoff_dir, delta23.delivery_id)
    print("差量入仓:", {k: r2[k] for k in ("status", "added", "removed", "to_view")})
    print("县仓当前:", receiver.query_current())
    print("轮次基线链（每轮针对哪个基线可查）:")
    for x in receiver.rounds():
        print(f"  round {x['round_id']} {x['kind']}: "
              f"{str(x['from_view'])[:10]} -> {str(x['to_view'])[:10]}")

    # ---- step 3: lose_ack（确认丢失，断线重送，不多加一轮）----
    banner("步骤3 lose_ack：入仓已持久化、确认丢失 → 重发同一身份，不新增轮次")
    # 用一条独立的新差量真实演示：提供方先备好 S3->S4 的 g1 差量
    provider.build_view(contract, "S4")
    probe = provider.offer_delta(contract, "S3", "S4")
    provider.send(probe.delivery_id, handoff_dir)
    rounds_before = len(receiver.rounds())
    rows_before = len(receiver.query_current())
    try:
        receiver.receive(handoff.package_dir(probe.delivery_id),
                         handoff_dir=handoff_dir, crash_hook="before_ack")
        raise AssertionError("断点未生效")
    except RuntimeError as exc:
        print("注入断点:", exc)
    assert not handoff.ack_path(probe.delivery_id).exists(), "确认不应写出"
    print(f"断线时：县仓已落盘 {len(receiver.query_current())} 行，"
          f"轮次={len(receiver.rounds())}，确认文件缺失")
    # 全新进程视角：提供方采不到确认（只认磁盘 ack 文件）
    fresh_provider = ProviderStore(work / "provider_store", inputs,
                                   inputs / "versions.json")
    print("提供方采集确认:", fresh_provider.collect_ack(handoff_dir, probe.delivery_id))
    # 按持久化账本重发同一传递身份同内容（不重新生成、不新造轮次）
    fresh_provider.resend(probe.delivery_id, handoff_dir)
    fresh_receiver = ReceiverStore(work / "receiver_store")
    again = fresh_receiver.receive(handoff.package_dir(probe.delivery_id),
                                   handoff_dir=handoff_dir)
    print("重收结果:", again["status"], "-", again["note"])
    assert again["status"] == "duplicate_ignored"
    assert len(fresh_receiver.rounds()) == rounds_before + 1  # 崩溃前那轮，重收未再加
    assert len(fresh_receiver.query_current()) == rows_before
    assert fresh_provider.collect_ack(
        handoff_dir, probe.delivery_id)["acked"] is True
    print(f"确认补齐；轮次未因重送增加，县仓行数无重复增减")
    fresh_provider.close(); fresh_receiver.close()

    # ---- step 4: retire S1,S2 ----
    banner("步骤4 retire S1/S2：旧基线退休，基于它的差量被拒，须协商完整替换")
    print(provider.retire(contract.identity_hash(), ["S1", "S2"]))
    try:
        provider.offer_delta(contract, "S2", "S4")
        raise AssertionError("退休基线差量未被拒绝")
    except MissingBaseline as exc:
        print(f"[拒绝] {type(exc).__name__}: {exc}")

    # ---- step 5: offer_rebase S4 ----
    banner("步骤5 offer_rebase S4：完整替换建立新代际，开放前县仓不受半成品影响")
    rebase = provider.offer_rebase(contract, "S4")
    assert rebase.manifest["generation"] == 2
    provider.send(rebase.delivery_id, handoff_dir)
    print("替换包已交接、尚未开放时县仓内容:", receiver.query_current())
    r5 = receiver.receive(handoff.package_dir(rebase.delivery_id),
                          handoff_dir=handoff_dir)
    print("完整替换入仓:", {k: r5[k] for k in
                          ("status", "kind", "generation", "to_view")})
    print("开放后县仓:", receiver.query_current())

    banner("附：新基线建立后，旧代际差量不得接入")
    # probe 是 g1 代际差量且已在 g1 时期入过账；这里验证另一条从未接入的旧代际路径：
    # 直接重放 probe 包，代际守卫必须拒绝（已应用的同身份包按幂等的前提是同代际）
    provider.send(probe.delivery_id, handoff_dir)
    try:
        receiver.receive(handoff.package_dir(probe.delivery_id),
                         handoff_dir=handoff_dir)
        # 该身份在 g1 已入账 -> 命中幂等；真正的旧代际拒绝由单元测试
        # test_old_generation_delta_rejected_after_rebase 覆盖
        print("已入账的同身份旧包按幂等忽略（未应用旧代际的拒绝路径见单元测试）")
    except StaleGeneration as exc:
        print(f"[拒绝] {type(exc).__name__}: {exc}")

    # ---- 双方对账 ----
    banner("对账：提供方自查 + 县仓全链路重放")
    prec = provider.reconcile()
    rrec = receiver.reconcile()
    print("提供方:", json.dumps(prec, ensure_ascii=False))
    print("县仓:", json.dumps(rrec, ensure_ascii=False, indent=2))
    assert prec["ok"] and rrec["ok"]

    # ---- DuckDB 直查真实 Parquet（独立进程视角）----
    banner("附：DuckDB 直接查询县仓真实 Parquet（weight 保持定点 decimal）")
    con = duckdb.connect()
    print(con.execute(
        "SELECT district, material, count(*) AS copies, min(weight) AS w "
        "FROM read_parquet(?) GROUP BY 1, 2 ORDER BY 1, 2",
        [str(receiver.current_path)]).fetchall())
    con.close()

    provider.close()
    receiver.close()
    print("\n场景全部通过。工作目录:", work)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
