from __future__ import annotations

import argparse
import csv
import json
import struct
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import pyodbc

from deploy_to_fabric import FABRIC_API, FabricClient, azure_token
from generate_netezza_data import NULL_VALUE, TABLE_FIELDS, write_dataset


SQL_COPT_SS_ACCESS_TOKEN = 1256

TABLE_DDL = {
    "customer_dim": """
        CREATE TABLE dbo.customer_dim (
            customer_id BIGINT NOT NULL,
            customer_name VARCHAR(120) NOT NULL,
            industry VARCHAR(60) NOT NULL,
            country_code CHAR(2) NOT NULL,
            customer_segment VARCHAR(30) NOT NULL,
            loyalty_tier VARCHAR(20) NULL,
            created_date DATE NOT NULL
        )
    """,
    "product_dim": """
        CREATE TABLE dbo.product_dim (
            product_id BIGINT NOT NULL,
            sku VARCHAR(30) NOT NULL,
            product_name VARCHAR(120) NOT NULL,
            category VARCHAR(60) NOT NULL,
            unit_price DECIMAL(12,2) NOT NULL,
            active_flag BIT NOT NULL
        )
    """,
    "order_fact": """
        CREATE TABLE dbo.order_fact (
            order_id BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            order_timestamp DATETIME2(0) NOT NULL,
            order_status VARCHAR(20) NOT NULL,
            sales_channel VARCHAR(20) NOT NULL,
            currency_code CHAR(3) NOT NULL,
            order_total DECIMAL(14,2) NOT NULL
        )
    """,
    "order_line_fact": """
        CREATE TABLE dbo.order_line_fact (
            order_line_id BIGINT NOT NULL,
            order_id BIGINT NOT NULL,
            product_id BIGINT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(12,2) NOT NULL,
            discount_pct DECIMAL(5,2) NOT NULL,
            line_total DECIMAL(14,2) NOT NULL
        )
    """,
}

CONVERTERS: dict[str, dict[str, Callable[[str], Any]]] = {
    "customer_dim": {
        "customer_id": int,
        "created_date": date.fromisoformat,
    },
    "product_dim": {
        "product_id": int,
        "unit_price": Decimal,
        "active_flag": lambda value: value.casefold() == "true",
    },
    "order_fact": {
        "order_id": int,
        "customer_id": int,
        "order_timestamp": lambda value: datetime.strptime(
            value,
            "%Y-%m-%d %H:%M:%S",
        ),
        "order_total": Decimal,
    },
    "order_line_fact": {
        "order_line_id": int,
        "order_id": int,
        "product_id": int,
        "quantity": int,
        "unit_price": Decimal,
        "discount_pct": Decimal,
        "line_total": Decimal,
    },
}


def create_warehouse(
    client: FabricClient,
    workspace_id: str,
    display_name: str,
) -> dict[str, Any]:
    existing = client.find_item(workspace_id, display_name, "Warehouse")
    if existing:
        item = existing
    else:
        response = client.fabric.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items",
            json={
                "displayName": display_name,
                "description": (
                    "Synthetic Netezza data migrated into native Fabric "
                    "Warehouse tables."
                ),
                "type": "Warehouse",
            },
        )
        if response.status_code == 201:
            item = response.json()
        else:
            client._poll_operation(response)
            item = client.find_item(workspace_id, display_name, "Warehouse")
            if not item:
                raise RuntimeError("Warehouse was not visible after creation.")

    response = client.fabric.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/warehouses/{item['id']}"
    )
    client._raise(response)
    return response.json()


def _odbc_driver() -> str:
    drivers = set(pyodbc.drivers())
    for candidate in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if candidate in drivers:
            return candidate
    raise RuntimeError(
        "Install Microsoft ODBC Driver 18 or 17 for SQL Server before loading "
        "the Fabric Warehouse."
    )


def connect_warehouse(
    endpoint: str,
    database: str,
    timeout_seconds: int = 600,
) -> pyodbc.Connection:
    token = azure_token("https://database.windows.net/").encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    connection_string = (
        f"Driver={{{_odbc_driver()}}};"
        f"Server=tcp:{endpoint},1433;"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    deadline = time.monotonic() + timeout_seconds
    last_error: pyodbc.OperationalError | None = None
    while time.monotonic() < deadline:
        try:
            return pyodbc.connect(
                connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                autocommit=False,
            )
        except pyodbc.OperationalError as error:
            last_error = error
            time.sleep(15)
    raise TimeoutError(
        f"Fabric Warehouse SQL endpoint was not ready within {timeout_seconds} seconds."
    ) from last_error


def read_export(export_dir: Path, table_name: str) -> list[tuple[Any, ...]]:
    fields = TABLE_FIELDS[table_name]
    converters = CONVERTERS[table_name]
    with (export_dir / f"{table_name}.tbl").open(
        newline="",
        encoding="utf-8",
    ) as handle:
        rows = []
        for source in csv.DictReader(handle, delimiter="|"):
            values = []
            for field in fields:
                value = source[field]
                if value == NULL_VALUE:
                    values.append(None)
                else:
                    values.append(converters.get(field, str)(value))
            rows.append(tuple(values))
    return rows


def load_tables(
    connection: pyodbc.Connection,
    export_dir: Path,
) -> dict[str, Any]:
    cursor = connection.cursor()
    for table_name in ("order_line_fact", "order_fact", "product_dim", "customer_dim"):
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")

    for table_name in ("customer_dim", "product_dim", "order_fact", "order_line_fact"):
        cursor.execute(TABLE_DDL[table_name])
        fields = TABLE_FIELDS[table_name]
        placeholders = ", ".join("?" for _ in fields)
        columns = ", ".join(fields)
        rows = read_export(export_dir, table_name)
        cursor.fast_executemany = True
        cursor.executemany(
            f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})",
            rows,
        )
    connection.commit()

    row_counts = {}
    for table_name in TABLE_FIELDS:
        row_counts[table_name] = cursor.execute(
            f"SELECT COUNT_BIG(*) FROM dbo.{table_name}"
        ).fetchone()[0]

    sales = cursor.execute(
        """
        SELECT
            (SELECT SUM(order_total) FROM dbo.order_fact),
            (SELECT SUM(line_total) FROM dbo.order_line_fact)
        """
    ).fetchone()
    orphans = cursor.execute(
        """
        SELECT
            (SELECT COUNT_BIG(*)
             FROM dbo.order_fact AS o
             LEFT JOIN dbo.customer_dim AS c
               ON o.customer_id = c.customer_id
             WHERE c.customer_id IS NULL),
            (SELECT COUNT_BIG(*)
             FROM dbo.order_line_fact AS l
             LEFT JOIN dbo.order_fact AS o
               ON l.order_id = o.order_id
             WHERE o.order_id IS NULL),
            (SELECT COUNT_BIG(*)
             FROM dbo.order_line_fact AS l
             LEFT JOIN dbo.product_dim AS p
               ON l.product_id = p.product_id
             WHERE p.product_id IS NULL)
        """
    ).fetchone()
    return {
        "row_counts": row_counts,
        "order_sales": f"{sales[0]:.2f}",
        "line_sales": f"{sales[1]:.2f}",
        "orphan_orders": orphans[0],
        "orphan_lines": orphans[1],
        "orphan_products": orphans[2],
    }


def validate_report(
    report: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    if report["row_counts"] != expected["row_counts"]:
        raise RuntimeError(
            f"Warehouse row counts do not match the source manifest: {report}"
        )
    if report["order_sales"] != expected["net_sales"]:
        raise RuntimeError(
            f"Warehouse order sales do not match the source manifest: {report}"
        )
    if report["line_sales"] != expected["net_sales"]:
        raise RuntimeError(
            f"Warehouse line sales do not match the source manifest: {report}"
        )
    orphan_counts = {
        key: report[key]
        for key in ("orphan_orders", "orphan_lines", "orphan_products")
    }
    if any(orphan_counts.values()):
        raise RuntimeError(f"Warehouse contains orphan records: {orphan_counts}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic Netezza exports and load them into a Fabric Warehouse."
        )
    )
    parser.add_argument(
        "--workspace-name",
        default="IBM Netezza Fabric Integration Demo",
    )
    parser.add_argument("--capacity-id", required=True)
    parser.add_argument("--warehouse-name", default="NetezzaMigrationWarehouse")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    export_dir = root / "data" / "netezza_export"
    manifest = write_dataset(export_dir)
    client = FabricClient()
    workspace = client.create_workspace(args.workspace_name, args.capacity_id)
    warehouse = create_warehouse(client, workspace["id"], args.warehouse_name)
    endpoint = warehouse["properties"]["connectionString"]

    with connect_warehouse(endpoint, args.warehouse_name) as connection:
        report = load_tables(connection, export_dir)
    validate_report(report, manifest["reconciliation"])

    state = {
        "workspace": workspace,
        "warehouse": warehouse,
        "source_reconciliation": manifest["reconciliation"],
        "warehouse_load_report": report,
    }
    (root / "warehouse-deployment-state.json").write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
