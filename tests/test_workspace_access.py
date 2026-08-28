"""Tests for Fabric workspace role assignment requests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from grant_workspace_access import grant_workspace_access  # noqa: E402


class WorkspaceAccessTests(unittest.TestCase):
    def test_grants_admin_role_to_user(self) -> None:
        response = Mock()
        response.json.return_value = {"id": "assignment-id", "role": "Admin"}
        client = Mock()
        client.fabric.post.return_value = response

        assignment = grant_workspace_access(
            client,
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
            "User",
            "Admin",
        )

        client.fabric.post.assert_called_once_with(
            (
                "https://api.fabric.microsoft.com/v1/workspaces/"
                "11111111-1111-1111-1111-111111111111/roleAssignments"
            ),
            json={
                "principal": {
                    "id": "22222222-2222-2222-2222-222222222222",
                    "type": "User",
                },
                "role": "Admin",
            },
        )
        client._raise.assert_called_once_with(response)
        self.assertEqual(assignment["role"], "Admin")


if __name__ == "__main__":
    unittest.main()
