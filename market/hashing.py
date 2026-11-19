"""规范化哈希与多重集工具。

无业务主键：相同 (day, district, material, weight) 的行可合法重复，
因此身份单位是“行内容的规范化哈希”，重复以重数解释，绝不整行去重。

精确小数 weight 为 decimal(18,3)，哈希时使用 Decimal 的定点标量编码，
不转浮点；NULL 与类型标记也是规范化编码的一部分（契约的一部分）。
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

# DuckDB / 本协议使用的逻辑类型 -> 二进制标记
_TYPE_TAGS = {
    "string": b"S",
    "decimal(18,3)": b"D",
}


def canonical_json(obj: Any) -> bytes:
    """确定性 JSON 编码（排序键、紧凑分隔、UTF-8），用于清单与契约哈希。"""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _encode_value(logical_type: str, value: Any) -> bytes:
    if value is None:
        return b"\x00N"  # NULL 独立标记
    tag = _TYPE_TAGS[logical_type]
    if logical_type == "string":
        payload = str(value).encode("utf-8")
    elif logical_type.startswith("decimal"):
        if not isinstance(value, Decimal):
            # 源文件里是字符串时，先按 Decimal 精确解析（禁止 float 中转）
            value = Decimal(str(value))
        payload = format(value, "f").encode("ascii")
    else:  # pragma: no cover - 契约目前只声明上述类型
        raise ValueError(f"不支持的共享列类型: {logical_type}")
    # 长度前缀杜绝列/值拼接歧义
    return b"\x01" + tag + len(payload).to_bytes(4, "big") + payload


def row_hash(columns: Sequence[str], types: Sequence[str], row: Mapping[str, Any]) -> str:
    """单行按契约列顺序的规范化 SHA-256。"""
    h = hashlib.sha256()
    for name, logical_type in zip(columns, types):
        key = name.encode("utf-8")
        h.update(len(key).to_bytes(2, "big"))
        h.update(key)
        h.update(_encode_value(logical_type, row.get(name)))
    return h.hexdigest()


def multiset_hashes(
    columns: Sequence[str],
    types: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """行列表 -> {row_hash: 重数}。重复行保留全部份数。"""
    counts: dict[str, int] = {}
    for row in rows:
        rh = row_hash(columns, types, row)
        counts[rh] = counts.get(rh, 0) + 1
    return counts


def view_fingerprint(multiset: Mapping[str, int]) -> str:
    """整个视图多重集的内容指纹。

    对 (row_hash, multiplicity) 规范化后逐项哈希，再按确定顺序汇总；
    与文件路径、行组、行顺序完全无关 —— 物理重排不产生任何业务变化。
    """
    h = hashlib.sha256()
    h.update(b"VIEW-V1")
    h.update(len(multiset).to_bytes(4, "big"))
    for rh in sorted(multiset):
        mult = multiset[rh]
        if mult <= 0:
            raise ValueError("行重数必须为正")
        h.update(bytes.fromhex(rh))
        h.update(mult.to_bytes(8, "big"))
    return h.hexdigest()


def diff_multisets(
    before: Mapping[str, int], after: Mapping[str, int]
) -> tuple[dict[str, int], dict[str, int]]:
    """多重集净变化 (added, removed)，各自保留重复份数。

    仅按行哈希的重数差裁决：投影后撞行、字段更正退出筛选都自然表现为净增减，
    不做整行去重，也不使用 private_ticket 等未共享字段。
    """
    added: dict[str, int] = {}
    removed: dict[str, int] = {}
    for key in set(before) | set(after):
        delta = after.get(key, 0) - before.get(key, 0)
        if delta > 0:
            added[key] = delta
        elif delta < 0:
            removed[key] = -delta
    return added, removed


def apply_delta(
    base: Mapping[str, int],
    added: Mapping[str, int],
    removed: Mapping[str, int],
) -> dict[str, int]:
    """在基线上应用净变化；减量使重数为负即拒绝（契约硬性要求）。"""
    result = dict(base)
    for key, n in removed.items():
        cur = result.get(key, 0)
        if n > cur:
            raise ValueError(f"减量使行 {key[:12]}… 重数为负: 现有 {cur}, 要求减 {n}")
        if cur == n:
            del result[key]
        else:
            result[key] = cur - n
    for key, n in added.items():
        result[key] = result.get(key, 0) + n
    return result
