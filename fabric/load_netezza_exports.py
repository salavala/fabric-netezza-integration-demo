# Fabric notebook source
# METADATA ********************
# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json

from notebookutils import mssparkutils
from pyspark.sql import functions as F


lakehouse_root = "__LAKEHOUSE_ROOT__"
source_root = f"{lakehouse_root}/Files/netezza_export"

casts = {
    "customer_dim": {
        "customer_id": "long",
        "created_date": "date",
    },
    "product_dim": {
        "product_id": "long",
        "unit_price": "decimal(12,2)",
        "active_flag": "boolean",
    },
    "order_fact": {
        "order_id": "long",
        "customer_id": "long",
        "order_timestamp": "timestamp",
        "order_total": "decimal(14,2)",
    },
    "order_line_fact": {
        "order_line_id": "long",
        "order_id": "long",
        "product_id": "long",
        "quantity": "integer",
        "unit_price": "decimal(12,2)",
        "discount_pct": "decimal(5,2)",
        "line_total": "decimal(14,2)",
    },
}

frames = {}
for table_name, table_casts in casts.items():
    frame = (
        spark.read.option("header", True)
        .option("sep", "|")
        .option("nullValue", "\\N")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(f"{source_root}/{table_name}.tbl")
    )
    for column, data_type in table_casts.items():
        frame = frame.withColumn(column, F.col(column).cast(data_type))
    (
        frame.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .save(f"{lakehouse_root}/Tables/{table_name}")
    )
    frames[table_name] = frame

order_sales = frames["order_fact"].select(F.sum("order_total").alias("value")).first()["value"]
line_sales = frames["order_line_fact"].select(F.sum("line_total").alias("value")).first()["value"]
orphan_orders = frames["order_fact"].join(
    frames["customer_dim"],
    "customer_id",
    "left_anti",
).count()
orphan_lines = frames["order_line_fact"].join(
    frames["order_fact"].select("order_id"),
    "order_id",
    "left_anti",
).count()
orphan_products = frames["order_line_fact"].join(
    frames["product_dim"].select("product_id"),
    "product_id",
    "left_anti",
).count()

report = {
    "row_counts": {name: frame.count() for name, frame in frames.items()},
    "order_sales": str(order_sales),
    "line_sales": str(line_sales),
    "orphan_orders": orphan_orders,
    "orphan_lines": orphan_lines,
    "orphan_products": orphan_products,
}
assert order_sales == line_sales
assert orphan_orders == 0
assert orphan_lines == 0
assert orphan_products == 0

mssparkutils.fs.put(
    f"{lakehouse_root}/Files/validation/load_report.json",
    json.dumps(report, indent=2),
    True,
)
display(spark.createDataFrame([report["row_counts"]]))

