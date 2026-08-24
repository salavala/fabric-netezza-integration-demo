import json
from typing import Any
from urllib.parse import quote

import requests
from notebookutils import mssparkutils
from pyspark.sql import functions as F


ICEBERG_BASE_URL = "https://onelake.table.fabric.microsoft.com/iceberg"
WORKSPACE_ID = "__WORKSPACE_ID__"
LAKEHOUSE_ID = "__LAKEHOUSE_ID__"
WAREHOUSE_ID = "__WAREHOUSE_ID__"
LAKEHOUSE_ROOT = "__LAKEHOUSE_ROOT__"
EXPECTED_TABLES = {
    "customer_dim",
    "product_dim",
    "order_fact",
    "order_line_fact",
}


class OneLakeIcebergClient:
    def __init__(self, workspace_id: str, item_id: str, token: str) -> None:
        self.catalog_scope = f"{workspace_id}/{item_id}"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        response = self.session.get(
            f"{ICEBERG_BASE_URL}/v1/config",
            params={"warehouse": self.catalog_scope},
            timeout=60,
        )
        response.raise_for_status()
        self.config = response.json()
        self.prefix = self.config["overrides"]["prefix"]

    def get(self, path: str) -> dict[str, Any]:
        response = self.session.get(
            f"{ICEBERG_BASE_URL}/v1/{self.prefix}/{path}",
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def list_namespaces(self) -> list[str]:
        payload = self.get("namespaces")
        return [namespace[0] for namespace in payload.get("namespaces", [])]

    def list_tables(self, namespace: str) -> list[str]:
        payload = self.get(
            f"namespaces/{quote(namespace, safe='')}/tables"
        )
        return [
            identifier["name"]
            for identifier in payload.get("identifiers", [])
        ]

    def get_table(self, namespace: str, table_name: str) -> dict[str, Any]:
        return self.get(
            "namespaces/"
            f"{quote(namespace, safe='')}/tables/{quote(table_name, safe='')}"
        )


def read_item(
    item_name: str,
    item_id: str,
    item_type: str,
    token: str,
) -> dict[str, Any]:
    rest_catalog = OneLakeIcebergClient(WORKSPACE_ID, item_id, token)
    namespaces = rest_catalog.list_namespaces()
    result: dict[str, Any] = {
        "id": item_id,
        "display_name": item_name,
        "type": item_type,
        "catalog_scope": rest_catalog.catalog_scope,
        "namespaces": {},
    }
    discovered_tables = set()

    for namespace in namespaces:
        table_names = rest_catalog.list_tables(namespace)
        discovered_tables.update(table_names)
        tables = {}
        frames = {}
        for table_name in table_names:
            metadata = rest_catalog.get_table(namespace, table_name)
            current_schema_id = metadata["metadata"]["current-schema-id"]
            current_schema = next(
                schema
                for schema in metadata["metadata"]["schemas"]
                if schema["schema-id"] == current_schema_id
            )
            tables[table_name] = {
                "metadata_location": metadata["metadata-location"],
                "table_location": metadata["metadata"]["location"],
                "format_version": metadata["metadata"]["format-version"],
                "columns": [
                    {
                        "name": field["name"],
                        "type": str(field["type"]),
                        "required": field["required"],
                    }
                    for field in current_schema["fields"]
                ],
            }
            frames[table_name] = (
                spark.read.format("delta")
                .load(metadata["metadata"]["location"])
            )

        query_results: dict[str, Any] = {
            "row_counts": {
                table_name: frame.count()
                for table_name, frame in frames.items()
            },
            "sample_rows": {
                table_name: [
                    json.loads(row)
                    for row in frame.limit(3).toJSON().collect()
                ]
                for table_name, frame in frames.items()
            },
        }
        if {"order_fact", "order_line_fact"}.issubset(frames):
            order_sales = frames["order_fact"].select(
                F.sum("order_total").alias("value")
            ).first()["value"]
            line_sales = frames["order_line_fact"].select(
                F.sum("line_total").alias("value")
            ).first()["value"]
            query_results["order_sales"] = f"{order_sales:.2f}"
            query_results["line_sales"] = f"{line_sales:.2f}"

        result["namespaces"][namespace] = {
            "tables": tables,
            "query_results": query_results,
        }

    missing_tables = sorted(EXPECTED_TABLES - discovered_tables)
    result["missing_expected_tables"] = missing_tables
    result["status"] = "ready" if not missing_tables else "incomplete"
    return result

storage_token = mssparkutils.credentials.getToken("storage")
storage_token = mssparkutils.credentials.getToken("storage")
report = {
    "workspace_id": WORKSPACE_ID,
    "items": {
        "lakehouse": read_item(
            "NetezzaMigrationLakehouse",
            LAKEHOUSE_ID,
            "Lakehouse",
            storage_token,
        ),
        "warehouse": read_item(
            "NetezzaMigrationWarehouse",
            WAREHOUSE_ID,
            "Warehouse",
            storage_token,
        ),
    },
}

lakehouse_results = report["items"]["lakehouse"]["namespaces"]["dbo"][
    "query_results"
]
assert lakehouse_results["row_counts"] == {
    "customer_dim": 200,
    "product_dim": 40,
    "order_fact": 1500,
    "order_line_fact": 4440,
}
assert lakehouse_results["order_sales"] == "12333986.08"
assert lakehouse_results["line_sales"] == "12333986.08"

report_json = json.dumps(report, indent=2, default=str)
mssparkutils.fs.put(
    f"{LAKEHOUSE_ROOT}/Files/validation/iceberg_api_notebook_report.json",
    report_json,
    True,
)
print(report_json)

summary = [
    {
        "item": item["display_name"],
        "type": item["type"],
        "status": item["status"],
        "table_count": sum(
            len(namespace["tables"])
            for namespace in item["namespaces"].values()
        ),
    }
    for item in report["items"].values()
]
display(spark.createDataFrame(summary))
