"""Tests for safe Iceberg identifiers and generated Fabric notebook content."""

from __future__ import annotations

import base64
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from read_with_iceberg_api import _validate_identifier  # noqa: E402
from deploy_table_api_demo import notebook_definition  # noqa: E402


class IcebergApiTests(unittest.TestCase):
    def test_accepts_catalog_identifiers(self) -> None:
        self.assertIsNone(_validate_identifier("order_line_fact"))

    def test_rejects_unsafe_catalog_identifiers(self) -> None:
        with self.assertRaises(ValueError):
            _validate_identifier("dbo.order_fact; DROP TABLE")

    def test_fabric_notebook_uses_attached_pyiceberg_environment(self) -> None:
        definition = notebook_definition(
            ROOT / "fabric" / "read_with_iceberg_api.py",
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            "33333333-3333-3333-3333-333333333333",
            "44444444-4444-4444-4444-444444444444",
        )
        notebook = json.loads(
            base64.b64decode(definition["parts"][0]["payload"]).decode("utf-8")
        )
        self.assertEqual(
            notebook["metadata"]["dependencies"]["environment"]["environmentId"],
            "44444444-4444-4444-4444-444444444444",
        )
        source = "".join(notebook["cells"][0]["source"])
        self.assertIn("from pyiceberg.catalog import load_catalog", source)
        self.assertIn("table.scan().to_arrow()", source)
        self.assertNotIn('spark.read.format("delta")', source)

    def test_fabric_notebook_allows_lakehouse_without_warehouse(self) -> None:
        definition = notebook_definition(
            ROOT / "fabric" / "read_with_iceberg_api.py",
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            None,
            "44444444-4444-4444-4444-444444444444",
        )
        notebook = json.loads(
            base64.b64decode(definition["parts"][0]["payload"]).decode("utf-8")
        )
        source = "".join(notebook["cells"][0]["source"])
        self.assertIn('WAREHOUSE_ID = ""', source)
        self.assertIn("if WAREHOUSE_ID:", source)


if __name__ == "__main__":
    unittest.main()
