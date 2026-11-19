"""命令行操作入口：共享申请、净变化生成、本地交接、接收入仓、查询与对账。

每个子命令都打开各自持久化目录中的 DuckDB/Parquet，独立进程之间不共享变量；
“是否已发送/已接收”一律以磁盘账本与确认文件裁决。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import MarketError
from .handoff import Handoff
from .provider import ProviderStore
from .receiver import ReceiverStore


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=_json_default))


def _json_default(value):
    from decimal import Decimal
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"不可序列化类型 {type(value)}")


def _provider(args) -> ProviderStore:
    return ProviderStore(args.provider_store, args.inputs, args.versions)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="market", description="无主键集市的差量共享接收")
    p.add_argument("--provider-store", default="work/provider_store")
    p.add_argument("--receiver-store", default="work/receiver_store")
    p.add_argument("--handoff", default="work/handoff")
    p.add_argument("--inputs", default="inputs")
    p.add_argument("--versions", default="inputs/versions.json")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("apply", help="接收方提交共享申请")
    sp.add_argument("--contract", required=True)
    sp.add_argument("--applicant", default="county-warehouse")

    sub.add_parser("applications", help="查看交接区共享申请")

    sp = sub.add_parser("accept", help="提供方接纳申请中的共享契约")
    sp.add_argument("--application", required=True)

    sp = sub.add_parser("adopt", help="接收方明确接纳申请中的共享契约")
    sp.add_argument("--application", required=True)

    sp = sub.add_parser("snapshot", help="生成首轮完整基线快照")
    sp.add_argument("--contract", required=True)
    sp.add_argument("--snapshot", required=True)

    sp = sub.add_parser("delta", help="生成两个基线快照间的净变化差量")
    sp.add_argument("--contract", required=True)
    sp.add_argument("--from-snapshot", required=True)
    sp.add_argument("--to-snapshot", required=True)

    sp = sub.add_parser("rebase", help="完整替换，建立新基线代际")
    sp.add_argument("--contract", required=True)
    sp.add_argument("--snapshot", required=True)

    sp = sub.add_parser("retire", help="提供方退休有限保留的旧版本")
    sp.add_argument("--snapshots", nargs="+", required=True)
    sp.add_argument("--contract", required=True)

    sp = sub.add_parser("send", help="把已持久化传递包放入本地交接区")
    sp.add_argument("--delivery-id", required=True)

    sp = sub.add_parser("receive", help="接收方核验并原子入仓")
    sp.add_argument("--delivery-id", required=True)
    sp.add_argument("--crash-hook", choices=["before_publish", "before_ack"],
                    default=None, help="注入断点（验收断线重送）")

    sp = sub.add_parser("collect-ack", help="提供方采集接收确认并记账")
    sp.add_argument("--delivery-id", required=True)

    sub.add_parser("provider-ledger", help="提供方传递账本")
    sub.add_parser("provider-status", help="提供方契约/代际/开放基线状态")
    sub.add_parser("provider-reconcile", help="提供方自查对账")
    sub.add_parser("receiver-rounds", help="接收方轮次（每轮针对哪个基线可查）")
    sub.add_parser("receiver-conflicts", help="接收方冲突留痕")
    sub.add_parser("receiver-state", help="接收方当前基线与重数")
    sub.add_parser("receiver-reconcile", help="接收方全链路对账")

    sp = sub.add_parser("query", help="对县仓当前开放版本执行 DuckDB SQL")
    sp.add_argument("--sql", default=None)

    sp = sub.add_parser("query-round", help="查询某一轮留档 Parquet")
    sp.add_argument("--round-id", type=int, required=True)
    sp.add_argument("--sql", default=None)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cmd = args.cmd
    try:
        if cmd == "apply":
            handoff = Handoff(args.handoff)
            path = handoff.submit_application(args.contract, args.applicant)
            _print({"submitted": str(path)})
            return 0

        if cmd == "applications":
            _print(Handoff(args.handoff).list_applications())
            return 0

        if cmd == "accept":
            application = json.loads(Path(args.application).read_text(encoding="utf-8"))
            provider = _provider(args)
            ch = provider.accept_application(application)
            _print({"accepted_contract_hash": ch})
            provider.close()
            return 0

        if cmd == "adopt":
            application = json.loads(Path(args.application).read_text(encoding="utf-8"))
            receiver = ReceiverStore(args.receiver_store)
            ch = receiver.adopt_contract(application)
            _print({"adopted_contract_hash": ch})
            receiver.close()
            return 0

        if cmd in ("snapshot", "delta", "rebase"):
            from .contract import ShareContract
            provider = _provider(args)
            contract = ShareContract.from_file(args.contract)
            if cmd == "snapshot":
                pkg = provider.offer_snapshot(contract, args.snapshot)
            elif cmd == "delta":
                pkg = provider.offer_delta(
                    contract, args.from_snapshot, args.to_snapshot)
            else:
                pkg = provider.offer_rebase(contract, args.snapshot)
            _print({"delivery_id": pkg.delivery_id, "kind": pkg.manifest["kind"],
                    "generation": pkg.manifest["generation"],
                    "from_view": pkg.manifest.get("from_view"),
                    "to_view": pkg.manifest["to_view"],
                    "counts": pkg.manifest["counts"],
                    "content_hash": pkg.content_hash,
                    "package": str(pkg.root)})
            provider.close()
            return 0

        if cmd == "retire":
            provider = _provider(args)
            from .contract import ShareContract
            ch = ShareContract.from_file(args.contract).identity_hash()
            _print(provider.retire(ch, args.snapshots))
            provider.close()
            return 0

        if cmd == "send":
            provider = _provider(args)
            handoff = Handoff(args.handoff)
            pkg = provider.send(args.delivery_id, handoff.root)
            _print({"sent": pkg.delivery_id, "handoff_package": str(pkg.root),
                    "content_hash": pkg.content_hash})
            provider.close()
            return 0

        if cmd == "receive":
            handoff = Handoff(args.handoff)
            receiver = ReceiverStore(args.receiver_store)
            pkg_dir = handoff.package_dir(args.delivery_id)
            result = receiver.receive(pkg_dir, handoff_dir=handoff.root,
                                      crash_hook=args.crash_hook)
            _print(result)
            receiver.close()
            return 0

        if cmd == "collect-ack":
            provider = _provider(args)
            _print(provider.collect_ack(args.handoff, args.delivery_id))
            provider.close()
            return 0

        if cmd == "provider-ledger":
            provider = _provider(args)
            _print(provider.ledger())
            provider.close()
            return 0

        if cmd == "provider-status":
            provider = _provider(args)
            _print(provider.status())
            provider.close()
            return 0

        if cmd == "provider-reconcile":
            provider = _provider(args)
            _print(provider.reconcile())
            provider.close()
            return 0

        if cmd == "receiver-rounds":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.rounds())
            receiver.close()
            return 0

        if cmd == "receiver-conflicts":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.conflicts())
            receiver.close()
            return 0

        if cmd == "receiver-state":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.state())
            receiver.close()
            return 0

        if cmd == "receiver-reconcile":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.reconcile())
            receiver.close()
            return 0

        if cmd == "query":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.query_current(args.sql))
            receiver.close()
            return 0

        if cmd == "query-round":
            receiver = ReceiverStore(args.receiver_store)
            _print(receiver.query_round(args.round_id, args.sql))
            receiver.close()
            return 0

    except MarketError as exc:
        print(f"[拒绝] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        print(f"[错误] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0
