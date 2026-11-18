import json,unittest,tempfile,shutil
from decimal import Decimal
from pathlib import Path
import pyarrow.parquet as pq
from materials.physical import load_inventory
ROOT=Path(__file__).resolve().parents[1]

class InputMaterials(unittest.TestCase):
    def test_real_files_and_independent_values(self):
        records=load_inventory(ROOT/"inputs")
        self.assertTrue(records)
        seed=json.loads((ROOT/"inputs/seed.json").read_text(encoding="utf-8"))
        for name,rows in seed.items():
            self.assertEqual(pq.read_table(ROOT/"inputs"/(name+".parquet")).to_pylist(),rows)

    def test_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/"inputs";shutil.copytree(ROOT/"inputs",target)
            inv=json.loads((target/"inventory.json").read_text(encoding="utf-8"))
            p=target/inv["files"][0]["path"];p.write_bytes(p.read_bytes()+b"x")
            with self.assertRaisesRegex(ValueError,"摘要"):load_inventory(target)

    def test_business_material_shapes(self):
        seed=json.loads((ROOT/"inputs/seed.json").read_text(encoding="utf-8"))
        actions=json.loads((ROOT/"inputs/actions.json").read_text(encoding="utf-8"))
        self.assertEqual(len(seed['s2_repacked']),len(seed['s1_a'])+len(seed['s1_b']))
        self.assertEqual(seed['s1_a'][0]['weight'],seed['s1_a'][1]['weight'])
        self.assertNotEqual(seed['s1_a'][0]['private_ticket'],seed['s1_a'][1]['private_ticket'])
        self.assertEqual(seed['s3_changed'][-1]['district'],'C')
