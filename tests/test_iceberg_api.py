from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from read_with_iceberg_api import _validate_identifier  # noqa: E402


class IcebergApiTests(unittest.TestCase):
    def test_accepts_catalog_identifiers(self) -> None:
        self.assertIsNone(_validate_identifier("order_line_fact"))

    def test_rejects_unsafe_catalog_identifiers(self) -> None:
        with self.assertRaises(ValueError):
            _validate_identifier("dbo.order_fact; DROP TABLE")


if __name__ == "__main__":
    unittest.main()
