"""Grant a Microsoft Entra principal access to an existing Fabric workspace."""

from __future__ import annotations

import argparse
import json

from deploy_to_fabric import FABRIC_API, FabricClient


WORKSPACE_ROLES = ("Admin", "Member", "Contributor", "Viewer")
PRINCIPAL_TYPES = ("User", "Group", "ServicePrincipal")


def grant_workspace_access(
    client: FabricClient,
    workspace_id: str,
    principal_id: str,
    principal_type: str,
    role: str,
) -> dict:
    """Add a workspace role assignment and return the created assignment."""

    response = client.fabric.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/roleAssignments",
        json={
            "principal": {
                "id": principal_id,
                "type": principal_type,
            },
            "role": role,
        },
    )
    client._raise(response)
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Grant a user, group, or service principal access to a workspace."
    )
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--principal-id", required=True)
    parser.add_argument(
        "--principal-type",
        choices=PRINCIPAL_TYPES,
        default="User",
    )
    parser.add_argument("--role", choices=WORKSPACE_ROLES, default="Admin")
    args = parser.parse_args()

    assignment = grant_workspace_access(
        FabricClient(),
        args.workspace_id,
        args.principal_id,
        args.principal_type,
        args.role,
    )
    print(json.dumps(assignment, indent=2))


if __name__ == "__main__":
    main()
