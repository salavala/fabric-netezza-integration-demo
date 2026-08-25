import json
from typing import Any

from notebookutils import mssparkutils
from pyiceberg.catalog import load_catalog


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


def load_onelake_catalog(item_id: str, token: str):
    catalog_scope = f"{WORKSPACE_ID}/{item_id}"
    return load_catalog(
        f"onelake_{item_id.replace('-', '')}",
        **{
            "uri": ICEBERG_BASE_URL,
            "warehouse": catalog_scope,
            "token": token,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "adls.account-name": "onelake",
            "adls.account-host": "onelake.blob.fabric.microsoft.com",
            "adls.token": token,
        },
    )


def read_item(
    item_name: str,
    item_id: str,
    item_type: str,
    token: str,
) -> dict[str, Any]:
    catalog_scope = f"{WORKSPACE_ID}/{item_id}"
    catalog = load_onelake_catalog(item_id, token)
    namespaces = [identifier[0] for identifier in catalog.list_namespaces()]
    result: dict[str, Any] = {
        "id": item_id,
        "display_name": item_name,
        "type": item_type,
        "catalog_scope": catalog_scope,
        "namespaces": {},
    }
    discovered_tables = set()

    for namespace in namespaces:
        table_names = [
            identifier[-1]
            for identifier in catalog.list_tables(namespace)
        ]
        discovered_tables.update(table_names)
        tables = {}
        frames = {}
        for table_name in table_names:
            table = catalog.load_table((namespace, table_name))
            schema = table.schema()
            tables[table_name] = {
                "metadata_location": table.metadata_location,
                "table_location": table.location(),
                "format_version": table.metadata.format_version,
                "columns": [
                    {
                        "name": field.name,
                        "type": str(field.field_type),
                        "required": field.required,
                    }
                    for field in schema.fields
                ],
            }
            frames[table_name] = table.scan().to_arrow()

        query_results: dict[str, Any] = {
            "row_counts": {
                table_name: len(frame)
                for table_name, frame in frames.items()
            },
            "sample_rows": {
                table_name: frame.slice(0, 3).to_pylist()
                for table_name, frame in frames.items()
            },
        }
        if {"order_fact", "order_line_fact"}.issubset(frames):
            order_sales = sum(frames["order_fact"]["order_total"].to_pylist())
            line_sales = sum(frames["order_line_fact"]["line_total"].to_pylist())
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
report = {
    "workspace_id": WORKSPACE_ID,
    "items": {
        "lakehouse": read_item(
            "NetezzaMigrationLakehouse",
            LAKEHOUSE_ID,
            "Lakehouse",
            storage_token,
        ),
    },
}
if WAREHOUSE_ID:
    report["items"]["warehouse"] = read_item(
        "NetezzaMigrationWarehouse",
        WAREHOUSE_ID,
        "Warehouse",
        storage_token,
    )

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
