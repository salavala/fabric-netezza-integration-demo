from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from azure.identity import AzureCliCredential
from pyiceberg.catalog import load_catalog

from deploy_to_fabric import FabricClient, azure_token


ICEBERG_BASE_URL = "https://onelake.table.fabric.microsoft.com/iceberg"
EXPECTED_TABLES = {
    "customer_dim",
    "product_dim",
    "order_fact",
    "order_line_fact",
}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class OneLakeIcebergClient:
    def __init__(self, workspace_id: str, item_id: str) -> None:
        self.warehouse = f"{workspace_id}/{item_id}"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": (
                    f"Bearer {azure_token('https://storage.azure.com/')}"
                )
            }
        )
        response = self.session.get(
            f"{ICEBERG_BASE_URL}/v1/config",
            params={"warehouse": self.warehouse},
            timeout=60,
        )
        self._raise(response)
        self.config = response.json()
        self.prefix = self.config["overrides"]["prefix"]

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

    def _get(self, path: str) -> dict[str, Any]:
        response = self.session.get(
            f"{ICEBERG_BASE_URL}/v1/{self.prefix}/{path}",
            timeout=60,
        )
        self._raise(response)
        return response.json()

    def list_namespaces(self) -> list[str]:
        payload = self._get("namespaces")
        return [namespace[0] for namespace in payload.get("namespaces", [])]

    def list_tables(self, namespace: str) -> list[str]:
        payload = self._get(
            f"namespaces/{quote(namespace, safe='')}/tables"
        )
        return [
            identifier["name"]
            for identifier in payload.get("identifiers", [])
        ]

    def get_table(self, namespace: str, table_name: str) -> dict[str, Any]:
        return self._get(
            "namespaces/"
            f"{quote(namespace, safe='')}/tables/{quote(table_name, safe='')}"
        )


def _validate_identifier(value: str) -> None:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe identifier returned by catalog: {value!r}")


def query_with_pyiceberg(
    workspace_id: str,
    item_id: str,
    namespace: str,
    table_names: list[str],
) -> dict[str, Any]:
    catalog_scope = f"{workspace_id}/{item_id}"
    credential = AzureCliCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    catalog = load_catalog(
        f"onelake_{item_id.replace('-', '')}",
        **{
            "uri": ICEBERG_BASE_URL,
            "token": token,
            "warehouse": catalog_scope,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "adls.account-name": "onelake",
            "adls.account-host": "onelake.blob.fabric.microsoft.com",
            "adls.credential": credential,
        },
    )

    frames = {}
    for table_name in sorted(table_names):
        _validate_identifier(table_name)
        table = catalog.load_table((namespace, table_name))
        frames[table_name] = table.scan().to_arrow()

    report: dict[str, Any] = {
        "row_counts": {
            table_name: len(frame)
            for table_name, frame in frames.items()
        },
        "sample_rows": {
            table_name: frame.slice(0, 3).to_pylist()
            for table_name, frame in frames.items()
        },
    }
    if {"order_fact", "order_line_fact"}.issubset(table_names):
        order_sales = sum(
            frames["order_fact"]["order_total"].to_pylist()
        )
        line_sales = sum(
            frames["order_line_fact"]["line_total"].to_pylist()
        )
        report["order_sales"] = f"{order_sales:.2f}"
        report["line_sales"] = f"{line_sales:.2f}"
    return report


def inspect_item(
    workspace_id: str,
    item: dict[str, Any],
    require_tables: bool,
) -> dict[str, Any]:
    catalog = OneLakeIcebergClient(workspace_id, item["id"])
    namespaces = catalog.list_namespaces()
    result: dict[str, Any] = {
        "id": item["id"],
        "display_name": item["displayName"],
        "type": item["type"],
        "catalog_scope": catalog.warehouse,
        "namespaces": {},
    }
    discovered_tables = set()

    for namespace in namespaces:
        tables = catalog.list_tables(namespace)
        discovered_tables.update(tables)
        metadata = {}
        for table_name in tables:
            table = catalog.get_table(namespace, table_name)
            current_schema_id = table["metadata"]["current-schema-id"]
            current_schema = next(
                schema
                for schema in table["metadata"]["schemas"]
                if schema["schema-id"] == current_schema_id
            )
            metadata[table_name] = {
                "metadata_location": table["metadata-location"],
                "format_version": table["metadata"]["format-version"],
                "columns": [
                    {
                        "name": field["name"],
                        "type": field["type"],
                        "required": field["required"],
                    }
                    for field in current_schema["fields"]
                ],
            }
        result["namespaces"][namespace] = {
            "tables": metadata,
            "query_results": query_with_pyiceberg(
                workspace_id,
                item["id"],
                namespace,
                tables,
            ),
        }

    missing_tables = sorted(EXPECTED_TABLES - discovered_tables)
    result["missing_expected_tables"] = missing_tables
    result["status"] = "ready" if not missing_tables else "incomplete"
    if require_tables and missing_tables:
        raise RuntimeError(
            f"{item['displayName']} is missing expected tables: {missing_tables}"
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Read Fabric Lakehouse and Warehouse tables through the OneLake "
            "Iceberg REST Catalog API."
        )
    )
    parser.add_argument(
        "--workspace-name",
        default="IBM Netezza Fabric Integration Demo",
    )
    parser.add_argument("--lakehouse-name", default="NetezzaMigrationLakehouse")
    parser.add_argument("--warehouse-name", default="NetezzaMigrationWarehouse")
    parser.add_argument(
        "--items",
        nargs="+",
        choices=("lakehouse", "warehouse"),
        default=("lakehouse", "warehouse"),
    )
    parser.add_argument(
        "--require-tables",
        action="store_true",
        help="Fail if any selected item does not contain all four demo tables.",
    )
    args = parser.parse_args()

    client = FabricClient()
    matches = [
        workspace
        for workspace in client.list_workspaces()
        if workspace["displayName"].casefold() == args.workspace_name.casefold()
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one workspace named {args.workspace_name!r}; found {len(matches)}."
        )
    workspace = matches[0]
    targets = {
        "lakehouse": (args.lakehouse_name, "Lakehouse"),
        "warehouse": (args.warehouse_name, "Warehouse"),
    }

    results = {}
    for target in args.items:
        display_name, item_type = targets[target]
        item = client.find_item(workspace["id"], display_name, item_type)
        if not item:
            raise RuntimeError(
                f"{item_type} {display_name!r} was not found in the workspace."
            )
        results[target] = inspect_item(
            workspace["id"],
            item,
            args.require_tables,
        )

    output = {
        "workspace": {
            "id": workspace["id"],
            "display_name": workspace["displayName"],
        },
        "items": results,
    }
    output_path = Path(__file__).resolve().parent / "iceberg-api-report.json"
    output_path.write_text(
        json.dumps(output, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
