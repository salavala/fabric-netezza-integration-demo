from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any


SEED = 20260824
AS_OF_DATE = date(2026, 8, 23)
NULL_VALUE = r"\N"
MONEY = Decimal("0.01")

TABLE_FIELDS = {
    "customer_dim": [
        "customer_id",
        "customer_name",
        "industry",
        "country_code",
        "customer_segment",
        "loyalty_tier",
        "created_date",
    ],
    "product_dim": [
        "product_id",
        "sku",
        "product_name",
        "category",
        "unit_price",
        "active_flag",
    ],
    "order_fact": [
        "order_id",
        "customer_id",
        "order_timestamp",
        "order_status",
        "sales_channel",
        "currency_code",
        "order_total",
    ],
    "order_line_fact": [
        "order_line_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "line_total",
    ],
}

TABLE_SCHEMAS = {
    "customer_dim": {
        "customer_id": "BIGINT",
        "customer_name": "VARCHAR(120)",
        "industry": "VARCHAR(60)",
        "country_code": "CHAR(2)",
        "customer_segment": "VARCHAR(30)",
        "loyalty_tier": "VARCHAR(20)",
        "created_date": "DATE",
    },
    "product_dim": {
        "product_id": "BIGINT",
        "sku": "VARCHAR(30)",
        "product_name": "VARCHAR(120)",
        "category": "VARCHAR(60)",
        "unit_price": "NUMERIC(12,2)",
        "active_flag": "BOOLEAN",
    },
    "order_fact": {
        "order_id": "BIGINT",
        "customer_id": "BIGINT",
        "order_timestamp": "TIMESTAMP",
        "order_status": "VARCHAR(20)",
        "sales_channel": "VARCHAR(20)",
        "currency_code": "CHAR(3)",
        "order_total": "NUMERIC(14,2)",
    },
    "order_line_fact": {
        "order_line_id": "BIGINT",
        "order_id": "BIGINT",
        "product_id": "BIGINT",
        "quantity": "INTEGER",
        "unit_price": "NUMERIC(12,2)",
        "discount_pct": "NUMERIC(5,2)",
        "line_total": "NUMERIC(14,2)",
    },
}

INDUSTRIES = (
    "Banking",
    "Healthcare",
    "Retail",
    "Manufacturing",
    "Telecommunications",
    "Insurance",
    "Energy",
    "Transportation",
)
COUNTRIES = ("US", "CA", "GB", "DE", "FR", "NL", "AU", "JP")
SEGMENTS = ("Enterprise", "Commercial", "Small Business")
CATEGORIES = ("Analytics", "Data Management", "Risk", "Fraud", "Customer Intelligence")
CHANNELS = ("Direct", "Partner", "Digital")
STATUSES = ("Completed", "Completed", "Completed", "Shipped", "Pending")


def _money(value: Decimal) -> str:
    return str(value.quantize(MONEY, rounding=ROUND_HALF_UP))


def build_tables() -> dict[str, list[dict[str, Any]]]:
    randomizer = random.Random(SEED)
    customers = []
    for customer_id in range(1, 201):
        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"Netezza Customer {customer_id:04d}",
                "industry": INDUSTRIES[(customer_id - 1) % len(INDUSTRIES)],
                "country_code": COUNTRIES[(customer_id * 3) % len(COUNTRIES)],
                "customer_segment": SEGMENTS[(customer_id * 5) % len(SEGMENTS)],
                "loyalty_tier": None if customer_id % 11 == 0 else ("Gold" if customer_id % 5 == 0 else "Standard"),
                "created_date": (date(2019, 1, 1) + timedelta(days=customer_id * 7)).isoformat(),
            }
        )

    products = []
    for product_id in range(1, 41):
        price = Decimal(75 + product_id * 18) + Decimal((product_id % 4) * 25) / 100
        products.append(
            {
                "product_id": product_id,
                "sku": f"NZ-SKU-{product_id:04d}",
                "product_name": f"Analytics Product {product_id:03d}",
                "category": CATEGORIES[(product_id - 1) % len(CATEGORIES)],
                "unit_price": _money(price),
                "active_flag": product_id % 17 != 0,
            }
        )

    orders = []
    order_lines = []
    product_by_id = {row["product_id"]: row for row in products}
    line_id = 1
    start = datetime(2025, 1, 1, 8, 0, 0)
    span_minutes = int((datetime(2026, 8, 23, 18, 0, 0) - start).total_seconds() // 60)
    for order_id in range(1, 1501):
        customer_id = randomizer.randint(1, len(customers))
        order_timestamp = start + timedelta(minutes=randomizer.randint(0, span_minutes))
        order_total = Decimal("0")
        line_count = randomizer.randint(1, 5)
        for _ in range(line_count):
            product_id = randomizer.randint(1, len(products))
            quantity = randomizer.randint(1, 12)
            unit_price = Decimal(product_by_id[product_id]["unit_price"])
            discount_pct = Decimal(randomizer.choice((0, 0, 0, 5, 10, 15)))
            line_total = (
                unit_price * quantity * (Decimal("1") - discount_pct / Decimal("100"))
            ).quantize(MONEY, rounding=ROUND_HALF_UP)
            order_total += line_total
            order_lines.append(
                {
                    "order_line_id": line_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": _money(unit_price),
                    "discount_pct": _money(discount_pct),
                    "line_total": _money(line_total),
                }
            )
            line_id += 1
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_timestamp": order_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "order_status": randomizer.choice(STATUSES),
                "sales_channel": randomizer.choice(CHANNELS),
                "currency_code": "USD",
                "order_total": _money(order_total),
            }
        )
    return {
        "customer_dim": customers,
        "product_dim": products,
        "order_fact": orders,
        "order_line_fact": order_lines,
    }


def validate_tables(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    customer_ids = {row["customer_id"] for row in tables["customer_dim"]}
    product_ids = {row["product_id"] for row in tables["product_dim"]}
    order_ids = {row["order_id"] for row in tables["order_fact"]}
    line_totals: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))

    for order in tables["order_fact"]:
        assert order["customer_id"] in customer_ids
    for line in tables["order_line_fact"]:
        assert line["order_id"] in order_ids
        assert line["product_id"] in product_ids
        line_totals[line["order_id"]] += Decimal(line["line_total"])
    for order in tables["order_fact"]:
        assert Decimal(order["order_total"]) == line_totals[order["order_id"]]

    net_sales = sum((Decimal(row["line_total"]) for row in tables["order_line_fact"]), Decimal("0"))
    gross_sales = sum(
        (
            Decimal(row["unit_price"]) * int(row["quantity"])
            for row in tables["order_line_fact"]
        ),
        Decimal("0"),
    )
    return {
        "as_of_date": AS_OF_DATE.isoformat(),
        "row_counts": {name: len(rows) for name, rows in tables.items()},
        "order_count": len(tables["order_fact"]),
        "customer_count": len(tables["customer_dim"]),
        "gross_sales": _money(gross_sales),
        "net_sales": _money(net_sales),
        "discount_amount": _money(gross_sales - net_sales),
    }


def _write_export(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(
            handle,
            delimiter="|",
            quotechar='"',
            escapechar="\\",
            lineterminator="\n",
        )
        writer.writerow(fields)
        for row in rows:
            writer.writerow(
                [NULL_VALUE if row[field] is None else str(row[field]) for field in fields]
            )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_dataset(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    tables = build_tables()
    reconciliation = validate_tables(tables)
    manifest: dict[str, Any] = {
        "format": "netezza_external_table_export_simulation",
        "seed": SEED,
        "delimiter": "|",
        "null_value": NULL_VALUE,
        "encoding": "utf-8",
        "reconciliation": reconciliation,
        "tables": {},
    }
    for table_name, rows in tables.items():
        target = output_dir / f"{table_name}.tbl"
        _write_export(target, TABLE_FIELDS[table_name], rows)
        manifest["tables"][table_name] = {
            "file": target.name,
            "rows": len(rows),
            "sha256": _sha256(target),
            "columns": TABLE_SCHEMAS[table_name],
        }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic IBM Netezza exports.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "netezza_export",
    )
    args = parser.parse_args()
    manifest = write_dataset(args.output)
    print(json.dumps(manifest["reconciliation"], indent=2))


if __name__ == "__main__":
    main()

