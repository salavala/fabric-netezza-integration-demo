from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from generate_netezza_data import build_tables, validate_tables, write_dataset  # noqa: E402


class NetezzaDataTests(unittest.TestCase):
    def test_reconciliation_and_references(self) -> None:
        report = validate_tables(build_tables())

        self.assertEqual(report["row_counts"]["customer_dim"], 200)
        self.assertEqual(report["row_counts"]["product_dim"], 40)
        self.assertEqual(report["row_counts"]["order_fact"], 1500)
        self.assertGreater(report["row_counts"]["order_line_fact"], 4000)
        self.assertGreater(float(report["net_sales"]), 0)
        self.assertGreater(float(report["discount_amount"]), 0)

    def test_generation_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_manifest = write_dataset(Path(first))
            second_manifest = write_dataset(Path(second))

        self.assertEqual(first_manifest["reconciliation"], second_manifest["reconciliation"])
        self.assertEqual(first_manifest["tables"], second_manifest["tables"])

    def test_manifest_matches_export_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = write_dataset(output)
            persisted = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(persisted, manifest)
            self.assertIn(r"\N", (output / "customer_dim.tbl").read_text(encoding="utf-8"))
            for table in manifest["tables"].values():
                self.assertTrue((output / table["file"]).is_file())


if __name__ == "__main__":
    unittest.main()
