from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from generate_netezza_data import write_dataset


FABRIC_API = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


def azure_token(resource: str) -> str:
    azure_cli = shutil.which("az.cmd") or shutil.which("az")
    if not azure_cli:
        raise RuntimeError("Azure CLI was not found on PATH.")
    result = subprocess.run(
        [
            azure_cli,
            "account",
            "get-access-token",
            "--resource",
            resource,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


class FabricClient:
    def __init__(self) -> None:
        self.fabric = requests.Session()
        self.fabric.headers.update(
            {
                "Authorization": f"Bearer {azure_token('https://api.fabric.microsoft.com')}",
                "Content-Type": "application/json",
            }
        )
        self.storage = requests.Session()
        self.storage.headers.update(
            {
                "Authorization": f"Bearer {azure_token('https://storage.azure.com/')}",
                "x-ms-version": "2023-11-03",
            }
        )

    @staticmethod
    def _raise(response: requests.Response) -> None:
        if response.ok:
            return
        try:
            detail = json.dumps(response.json(), indent=2)
        except ValueError:
            detail = response.text
        raise RuntimeError(
            f"{response.request.method} {response.url} failed "
            f"({response.status_code}): {detail}"
        )

    def _poll_operation(self, response: requests.Response, timeout_seconds: int = 900) -> None:
        if response.status_code != 202:
            self._raise(response)
            return
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError("Fabric accepted an operation without a Location header.")
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            time.sleep(min(max(int(response.headers.get("Retry-After", "5")), 2), 15))
            response = self.fabric.get(location)
            self._raise(response)
            status = response.json().get("status")
            if status in {"Succeeded", "Failed", "Cancelled"}:
                if status != "Succeeded":
                    raise RuntimeError(response.text)
                return
        raise TimeoutError(f"Fabric operation timed out: {location}")

    def list_workspaces(self) -> list[dict[str, Any]]:
        response = self.fabric.get(f"{FABRIC_API}/workspaces")
        self._raise(response)
        return response.json().get("value", [])

    def create_workspace(
        self,
        display_name: str,
        capacity_id: str,
    ) -> dict[str, Any]:
        matches = [
            workspace
            for workspace in self.list_workspaces()
            if workspace["displayName"].casefold() == display_name.casefold()
        ]
        if len(matches) > 1:
            raise RuntimeError(f"Multiple workspaces are named {display_name!r}.")
        if matches:
            workspace = matches[0]
        else:
            response = self.fabric.post(
                f"{FABRIC_API}/workspaces",
                json={
                    "displayName": display_name,
                    "description": (
                        "Self-contained IBM Netezza export modernization demo for "
                        "Microsoft Fabric Lakehouse."
                    ),
                    "capacityId": capacity_id,
                },
            )
            self._raise(response)
            workspace = response.json()

        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            response = self.fabric.get(f"{FABRIC_API}/workspaces/{workspace['id']}")
            self._raise(response)
            workspace = response.json()
            if workspace.get("capacityAssignmentProgress") == "Completed":
                break
            time.sleep(10)
        if workspace.get("capacityId") != capacity_id:
            raise RuntimeError(
                f"Workspace is assigned to {workspace.get('capacityId')}, not {capacity_id}."
            )
        return workspace

    def list_items(self, workspace_id: str, item_type: str) -> list[dict[str, Any]]:
        response = self.fabric.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            params={"type": item_type},
        )
        self._raise(response)
        return response.json().get("value", [])

    def find_item(
        self,
        workspace_id: str,
        display_name: str,
        item_type: str,
    ) -> dict[str, Any] | None:
        matches = [
            item
            for item in self.list_items(workspace_id, item_type)
            if item["displayName"].casefold() == display_name.casefold()
        ]
        if len(matches) > 1:
            raise RuntimeError(f"Multiple {item_type} items are named {display_name!r}.")
        return matches[0] if matches else None

    def create_lakehouse(self, workspace_id: str, display_name: str) -> dict[str, Any]:
        existing = self.find_item(workspace_id, display_name, "Lakehouse")
        if existing:
            return existing
        response = self.fabric.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            json={
                "displayName": display_name,
                "description": "Synthetic Netezza extracts and migrated Delta tables.",
                "type": "Lakehouse",
            },
        )
        if response.status_code == 201:
            return response.json()
        self._poll_operation(response)
        item = self.find_item(workspace_id, display_name, "Lakehouse")
        if not item:
            raise RuntimeError("Lakehouse was not visible after creation.")
        return item

    @staticmethod
    def notebook_definition(
        source_path: Path,
        workspace_id: str,
        lakehouse_id: str,
    ) -> dict[str, Any]:
        lakehouse_root = (
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
        )
        source = source_path.read_text(encoding="utf-8").replace(
            "__LAKEHOUSE_ROOT__",
            lakehouse_root,
        )
        notebook = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "cells": [
                {
                    "cell_type": "code",
                    "source": [line + "\n" for line in source.splitlines()],
                    "execution_count": None,
                    "outputs": [],
                    "metadata": {},
                }
            ],
            "metadata": {
                "language_info": {"name": "python"},
                "kernelspec": {
                    "name": "synapse_pyspark",
                    "display_name": "Synapse PySpark",
                    "language": "Python",
                },
            },
        }
        return {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": base64.b64encode(
                        json.dumps(notebook).encode("utf-8")
                    ).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
            ],
        }

    def upsert_notebook(
        self,
        workspace_id: str,
        display_name: str,
        definition: dict[str, Any],
    ) -> dict[str, Any]:
        existing = self.find_item(workspace_id, display_name, "Notebook")
        if existing:
            response = self.fabric.post(
                (
                    f"{FABRIC_API}/workspaces/{workspace_id}/items/{existing['id']}"
                    "/updateDefinition"
                ),
                json={"definition": definition},
            )
            self._poll_operation(response)
            return existing
        response = self.fabric.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            json={
                "displayName": display_name,
                "description": "Loads Netezza-style exports into Fabric Delta tables.",
                "type": "Notebook",
                "definition": definition,
            },
        )
        if response.status_code == 201:
            return response.json()
        self._poll_operation(response)
        item = self.find_item(workspace_id, display_name, "Notebook")
        if not item:
            raise RuntimeError("Notebook was not visible after creation.")
        return item

    def _storage_url(self, workspace_id: str, path: str, query: str = "") -> str:
        encoded = "/".join(quote(segment, safe="") for segment in path.split("/"))
        suffix = f"?{query}" if query else ""
        return f"{ONELAKE_DFS}/{workspace_id}/{encoded}{suffix}"

    def ensure_directory(
        self,
        workspace_id: str,
        lakehouse_id: str,
        relative_path: str,
    ) -> None:
        current = lakehouse_id
        for segment in relative_path.split("/"):
            current = f"{current}/{segment}"
            response = self.storage.put(
                self._storage_url(workspace_id, current, "resource=directory")
            )
            if response.status_code not in {201, 409}:
                self._raise(response)

    def upload_file(
        self,
        workspace_id: str,
        lakehouse_id: str,
        relative_path: str,
        content: bytes,
    ) -> None:
        path = f"{lakehouse_id}/{relative_path}"
        create = self.storage.put(
            self._storage_url(workspace_id, path, "resource=file")
        )
        if create.status_code == 409:
            self._raise(self.storage.delete(self._storage_url(workspace_id, path)))
            create = self.storage.put(
                self._storage_url(workspace_id, path, "resource=file")
            )
        self._raise(create)
        self._raise(
            self.storage.patch(
                self._storage_url(workspace_id, path, "action=append&position=0"),
                data=content,
                headers={"Content-Type": "application/octet-stream"},
            )
        )
        self._raise(
            self.storage.patch(
                self._storage_url(
                    workspace_id,
                    path,
                    f"action=flush&position={len(content)}",
                ),
                data=b"",
            )
        )

    def run_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        timeout_seconds: int = 1800,
    ) -> dict[str, Any]:
        response = self.fabric.post(
            (
                f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}"
                "/jobs/execute/instances?beta=false"
            )
        )
        self._raise(response)
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError("Notebook run did not return a Location header.")
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            time.sleep(20)
            response = self.fabric.get(location)
            self._raise(response)
            status = response.json()
            if status.get("status") in {"Completed", "Failed", "Cancelled", "Deduped"}:
                if status["status"] != "Completed":
                    raise RuntimeError(json.dumps(status, indent=2))
                return status
        raise TimeoutError("Notebook run did not complete.")

    def validate_delta_table(
        self,
        workspace_id: str,
        lakehouse_id: str,
        table_name: str,
    ) -> None:
        query = (
            "resource=filesystem&recursive=true&"
            f"directory={quote(f'{lakehouse_id}/Tables/{table_name}', safe='/')}"
        )
        response = self.storage.get(f"{ONELAKE_DFS}/{workspace_id}?{query}")
        self._raise(response)
        if not any(
            "_delta_log/" in entry.get("name", "")
            for entry in response.json().get("paths", [])
        ):
            raise RuntimeError(f"Delta table {table_name!r} has no transaction log.")

    def download_file(
        self,
        workspace_id: str,
        lakehouse_id: str,
        relative_path: str,
    ) -> bytes:
        response = self.storage.get(
            self._storage_url(workspace_id, f"{lakehouse_id}/{relative_path}")
        )
        self._raise(response)
        return response.content


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic Netezza exports and load them into a Fabric Lakehouse."
        )
    )
    parser.add_argument(
        "--workspace-name",
        default="IBM Netezza Fabric Integration Demo",
    )
    parser.add_argument("--capacity-id", required=True)
    parser.add_argument("--lakehouse-name", default="NetezzaMigrationLakehouse")
    parser.add_argument("--notebook-name", default="Load Netezza Synthetic Exports")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    export_dir = root / "data" / "netezza_export"
    manifest = write_dataset(export_dir)
    client = FabricClient()
    workspace = client.create_workspace(args.workspace_name, args.capacity_id)
    lakehouse = client.create_lakehouse(workspace["id"], args.lakehouse_name)

    client.ensure_directory(
        workspace["id"],
        lakehouse["id"],
        "Files/netezza_export",
    )
    client.ensure_directory(
        workspace["id"],
        lakehouse["id"],
        "Files/validation",
    )
    for source in sorted(export_dir.iterdir()):
        if source.is_file():
            client.upload_file(
                workspace["id"],
                lakehouse["id"],
                f"Files/netezza_export/{source.name}",
                source.read_bytes(),
            )

    notebook = client.upsert_notebook(
        workspace["id"],
        args.notebook_name,
        client.notebook_definition(
            root / "fabric" / "load_netezza_exports.py",
            workspace["id"],
            lakehouse["id"],
        ),
    )
    run = client.run_notebook(workspace["id"], notebook["id"])
    for table_name in manifest["tables"]:
        client.validate_delta_table(workspace["id"], lakehouse["id"], table_name)

    load_report = json.loads(
        client.download_file(
            workspace["id"],
            lakehouse["id"],
            "Files/validation/load_report.json",
        ).decode("utf-8")
    )
    expected = manifest["reconciliation"]
    if load_report["row_counts"] != expected["row_counts"]:
        raise RuntimeError(
            f"Fabric row counts do not match the source manifest: {load_report}"
        )
    if load_report["order_sales"] != expected["net_sales"]:
        raise RuntimeError(
            f"Fabric sales do not match the source manifest: {load_report}"
        )
    if load_report["line_sales"] != expected["net_sales"]:
        raise RuntimeError(
            f"Fabric line sales do not match the source manifest: {load_report}"
        )
    orphan_counts = {
        key: load_report[key]
        for key in ("orphan_orders", "orphan_lines", "orphan_products")
    }
    if any(orphan_counts.values()):
        raise RuntimeError(f"Fabric contains orphan records: {orphan_counts}")

    state = {
        "workspace": workspace,
        "lakehouse": lakehouse,
        "notebook": notebook,
        "notebook_run": run,
        "source_reconciliation": expected,
        "fabric_load_report": load_report,
    }
    (root / "deployment-state.json").write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
