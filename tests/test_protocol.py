"""协议基元测试：精确哈希、多重集差量、负数保护、契约身份与类型。"""
import unittest
from decimal import Decimal

from market.hashing import (
    apply_delta,
    diff_multisets,
    multiset_hashes,
    row_hash,
    view_fingerprint,
)
from market.contract import ShareContract

COLS = ("day", "district", "material", "weight")
TYPES = ("string", "string", "string", "decimal(18,3)")
CONTRACT = {"id": "share-v1", "columns": list(COLS), "types": list(TYPES),
            "where": {"district_in": ["A", "B"]}}


def r(day, district, material, weight):
    return {"day": day, "district": district, "material": material,
            "weight": weight}


class HashingTests(unittest.TestCase):
    def test_decimal_not_float_and_string_equivalence(self):
        h1 = row_hash(COLS, TYPES, r("d", "A", "m", "1.000"))
        h2 = row_hash(COLS, TYPES, r("d", "A", "m", Decimal("1.000")))
        self.assertEqual(h1, h2)  # 字符串与定点 Decimal 规范化一致
        h3 = row_hash(COLS, TYPES, r("d", "A", "m", Decimal("1.001")))
        self.assertNotEqual(h1, h3)

    def test_physical_reorder_same_fingerprint(self):
        rows1 = [r("1", "A", "p", "1.000"), r("1", "A", "p", "1.000"),
                 r("2", "B", "g", "2.000")]
        rows2 = list(reversed(rows1))
        fp1 = view_fingerprint(multiset_hashes(COLS, TYPES, rows1))
        fp2 = view_fingerprint(multiset_hashes(COLS, TYPES, rows2))
        self.assertEqual(fp1, fp2)  # 行顺序无关
        self.assertEqual(
            multiset_hashes(COLS, TYPES, rows1),
            multiset_hashes(COLS, TYPES, rows2),
        )

    def test_duplicates_kept_as_multiplicity(self):
        rows = [r("1", "A", "p", "1.000"), r("1", "A", "p", "1.000")]
        ms = multiset_hashes(COLS, TYPES, rows)
        self.assertEqual(sum(ms.values()), 2)  # 绝不整行去重

    def test_multiset_diff_net_changes(self):
        before = multiset_hashes(COLS, TYPES, [
            r("1", "A", "p", "1.000"), r("1", "A", "p", "1.000"),
            r("2", "B", "g", "2.000")])
        # 投影后撞行 + 一份退出：paper 两份变一份，glass 退出
        after = multiset_hashes(COLS, TYPES, [r("1", "A", "p", "1.000")])
        added, removed = diff_multisets(before, after)
        self.assertEqual(added, {})
        paper_h = row_hash(COLS, TYPES, r("1", "A", "p", "1.000"))
        glass_h = row_hash(COLS, TYPES, r("2", "B", "g", "2.000"))
        self.assertEqual(removed, {paper_h: 1, glass_h: 1})
        target = apply_delta(before, added, removed)
        self.assertEqual(view_fingerprint(target), view_fingerprint(after))

    def test_negative_multiplicity_rejected(self):
        h = row_hash(COLS, TYPES, r("1", "A", "p", "1.000"))
        with self.assertRaises(ValueError):
            apply_delta({h: 1}, {}, {h: 2})  # 减量使重数为负

    def test_contract_identity_changes_on_filter_column_type(self):
        base = ShareContract.from_dict(CONTRACT).identity_hash()
        wider = ShareContract.from_dict({**CONTRACT,
                                        "where": {"district_in": ["A", "B", "C"]}})
        self.assertNotEqual(base, wider.identity_hash())
        retyped = ShareContract.from_dict({**CONTRACT, "types":
                                           ["string", "string", "string", "string"]})
        self.assertNotEqual(base, retyped.identity_hash())
        fewer = ShareContract.from_dict({**CONTRACT,
                                         "columns": ["day", "district", "material"],
                                         "types": ["string", "string", "string"]})
        self.assertNotEqual(base, fewer.identity_hash())

    def test_contract_rejects_bad_shapes(self):
        with self.assertRaises(ValueError):
            ShareContract.from_dict({**CONTRACT, "columns": ["day"],
                                     "types": ["string", "string"]})
        with self.assertRaises(ValueError):
            ShareContract.from_dict({**CONTRACT,
                                     "types": ["string", "string", "string", "float"]})


if __name__ == "__main__":
    unittest.main()
