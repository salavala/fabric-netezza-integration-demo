from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from typing import Any

from deploy_to_fabric import FABRIC_API, FabricClient


EXPECTED_COUNTS = {
    "customer_dim": 200,
    "product_dim": 40,
    "order_fact": 1500,
    "order_line_fact": 4440,
}


def notebook_definition(
    source_path: Path,
    workspace_id: str,
    lakehouse_id: str,
    warehouse_id: str,
) -> dict[str, Any]:
    lakehouse_root = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
    )
    replacements = {
        "__WORKSPACE_ID__": workspace_id,
        "__LAKEHOUSE_ID__": lakehouse_id,
        "__WAREHOUSE_ID__": warehouse_id,
        "__LAKEHOUSE_ROOT__": lakehouse_root,
    }
    source = source_path.read_text(encoding="utf-8")
    for placeholder, value in replacements.items():
        source = source.replace(placeholder, value)

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
            },
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy and run the OneLake Iceberg Table API Fabric notebook."
    )
    parser.add_argument(
        "--workspace-name",
        default="IBM Netezza Fabric Integration Demo",
    )
    parser.add_argument("--lakehouse-name", default="NetezzaMigrationLakehouse")
    parser.add_argument("--warehouse-name", default="NetezzaMigrationWarehouse")
    parser.add_argument(
        "--notebook-name",
        default="OneLake Iceberg Table API Demo",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    client = FabricClient()
    workspaces = [
        workspace
        for workspace in client.list_workspaces()
        if workspace["displayName"].casefold() == args.workspace_name.casefold()
    ]
    if len(workspaces) != 1:
        raise RuntimeError(
            f"Expected one workspace named {args.workspace_name!r}; "
            f"found {len(workspaces)}."
        )
    workspace = workspaces[0]
    lakehouse = client.find_item(
        workspace["id"],
        args.lakehouse_name,
        "Lakehouse",
    )
    warehouse = client.find_item(
        workspace["id"],
        args.warehouse_name,
        "Warehouse",
    )
    if not lakehouse:
        raise RuntimeError(f"Lakehouse {args.lakehouse_name!r} was not found.")
    if not warehouse:
        raise RuntimeError(f"Warehouse {args.warehouse_name!r} was not found.")

    notebook = client.upsert_notebook(
        workspace["id"],
        args.notebook_name,
        notebook_definition(
            root / "fabric" / "read_with_iceberg_api.py",
            workspace["id"],
            lakehouse["id"],
            warehouse["id"],
        ),
    )
    response = client.fabric.patch(
        (
            f"{FABRIC_API}/workspaces/{workspace['id']}/items/"
            f"{notebook['id']}"
        ),
        json={
            "description": (
                "Discovers Lakehouse and Warehouse tables through the OneLake "
                "Iceberg REST Catalog API and reads their OneLake data."
            )
        },
    )
    client._raise(response)
    notebook = response.json()
    run = client.run_notebook(workspace["id"], notebook["id"])
    report = json.loads(
        client.download_file(
            workspace["id"],
            lakehouse["id"],
            "Files/validation/iceberg_api_notebook_report.json",
        ).decode("utf-8")
    )
    lakehouse_report = report["items"]["lakehouse"]
    query_results = lakehouse_report["namespaces"]["dbo"]["query_results"]
    if lakehouse_report["status"] != "ready":
        raise RuntimeError(f"Lakehouse Iceberg catalog is incomplete: {report}")
    if query_results["row_counts"] != EXPECTED_COUNTS:
        raise RuntimeError(f"Lakehouse Iceberg row counts do not match: {report}")
    if query_results["order_sales"] != "12333986.08":
        raise RuntimeError(f"Lakehouse Iceberg sales do not match: {report}")
    if query_results["line_sales"] != "12333986.08":
        raise RuntimeError(f"Lakehouse Iceberg line sales do not match: {report}")

    state = {
        "workspace": workspace,
        "lakehouse": lakehouse,
        "warehouse": warehouse,
        "notebook": notebook,
        "notebook_run": run,
        "table_api_report": report,
    }
    (root / "table-api-deployment-state.json").write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
