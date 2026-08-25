from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from deploy_to_fabric import FabricClient  # noqa: E402


class FabricDeploymentTests(unittest.TestCase):
    def test_create_workspace_rejects_existing_name(self) -> None:
        client = FabricClient.__new__(FabricClient)
        client.fabric = Mock()
        client.list_workspaces = Mock(
            return_value=[
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "displayName": "Netezza Demo",
                }
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "already exists"):
            client.create_workspace(
                "Netezza Demo",
                "22222222-2222-2222-2222-222222222222",
            )

        client.fabric.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
