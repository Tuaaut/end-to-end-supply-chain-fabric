# Fabric notebook source: nb_incremental_gold_facts_UAT
# Attach Lakehouse: lh_supply_chain_dev before running.
# Single Spark session: all Gold fact transformations run in dependency order.

from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

pipeline_run_id = ""
ORCHESTRATOR_NAME = "nb_incremental_gold_facts_merge_v2"
LOG_PIPELINE_TABLE = "ops_pipeline_run_log"
PIPELINE_RUN_ID = pipeline_run_id or datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ")

log_schema = T.StructType([
    T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("orchestrator_name", T.StringType(), False),
    T.StructField("notebook_name", T.StringType(), True),
    T.StructField("started_at", T.TimestampType(), False),
    T.StructField("ended_at", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("message", T.StringType(), True),
])

def append_log(status, message, started_at, ended_at=None):
    row = [(PIPELINE_RUN_ID, ORCHESTRATOR_NAME, None, started_at, ended_at, status, message)]
    spark.createDataFrame(row, log_schema).write.format("delta").mode("append").saveAsTable(LOG_PIPELINE_TABLE)


def merge_delta(df, table_name, keys):
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        return
    target = DeltaTable.forName(spark, table_name)
    source = df.dropDuplicates(keys)
    condition = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
    target_columns = set(target.toDF().columns)
    common_columns = [c for c in source.columns if c in target_columns]
    update_map = {c: f"s.`{c}`" for c in common_columns}
    insert_map = {c: f"s.`{c}`" for c in common_columns}
    (target.alias("t").merge(source.alias("s"), condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute())


def valid_source(table_name):
    source = spark.table(table_name)
    return source.filter("dq_status = 'VALID'") if "dq_status" in source.columns else source


INPUT_WATERMARK_TABLE = "ops_gold_input_watermark"
INPUT_WATERMARK_SCHEMA = T.StructType([
    T.StructField("process_name", T.StringType(), False),
    T.StructField("silver_watermark", T.TimestampType(), False),
])
_silver_wm = (spark.table("ops_incremental_watermark")
    .filter(F.col("process_name") == "nb_incremental_silver_merge_v2")
    .orderBy(F.col("last_success_at").desc()).limit(1).collect()) if spark.catalog.tableExists("ops_incremental_watermark") else []
CURRENT_SILVER_WM = _silver_wm[0]["last_success_at"] if _silver_wm else datetime(1970, 1, 1, tzinfo=timezone.utc)


def exit_if_no_new_silver():
    if spark.catalog.tableExists(INPUT_WATERMARK_TABLE):
        _previous = (spark.table(INPUT_WATERMARK_TABLE)
            .filter(F.col("process_name") == ORCHESTRATOR_NAME)
            .orderBy(F.col("silver_watermark").desc()).limit(1).collect())
        if _previous and CURRENT_SILVER_WM <= _previous[0]["silver_watermark"]:
            return True
    elif spark.catalog.tableExists("gld_fact_demand_forecast"):
        spark.createDataFrame([], INPUT_WATERMARK_SCHEMA).write.format("delta").saveAsTable(INPUT_WATERMARK_TABLE)
        spark.createDataFrame([(ORCHESTRATOR_NAME, CURRENT_SILVER_WM)], INPUT_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(INPUT_WATERMARK_TABLE)
        return True
    return False

stage_started = datetime.now(timezone.utc)
append_log("STARTED", "Starting consolidated Gold fact transformations", stage_started)
try:
    if exit_if_no_new_silver():
        append_log("SUCCESS", "No new Silver watermark; Gold facts MERGE skipped", stage_started, datetime.now(timezone.utc))
        try:
            notebookutils.notebook.exit("NO_CHANGES")
        except NameError:
            mssparkutils.notebook.exit("NO_CHANGES")
    # --- inlined from nb_gld_fact_demand_forecast.py ---
    df = valid_source("slv_demand_forecast").select("demand_date", "product_id", "location_id", "forecast_version", "forecast_qty", "actual_demand_qty", "forecast_error_qty", "demand_uom")
    merge_delta(df, "gld_fact_demand_forecast", ["demand_date", "product_id", "location_id", "forecast_version"])
    print("gld_fact_demand_forecast written")

    # --- inlined from nb_gld_fact_inventory_snapshot.py ---
    df = valid_source("slv_inventory_snapshot").select("snapshot_date", "product_id", "location_id", "on_hand_qty", "reserved_qty", "available_qty", "safety_stock_qty", "inventory_uom")
    merge_delta(df, "gld_fact_inventory_snapshot", ["snapshot_date", "product_id", "location_id"])
    print("gld_fact_inventory_snapshot written")

    # --- inlined from nb_gld_fact_purchase_receipt.py ---
    from pyspark.sql import functions as F
    receipts = valid_source("slv_purchase_order_receipt")
    orders = valid_source("slv_purchase_order").select("po_id", "supplier_id", "destination_location_id", "po_date", "unit_cost", "currency_code")
    df = receipts.join(orders, "po_id", "inner").withColumn("receipt_date", F.to_date("receipt_timestamp")).withColumn("receipt_variance_days", F.datediff(F.to_date("receipt_timestamp"), F.col("promised_date"))).select("receipt_id", "po_id", "product_id", "location_id", "supplier_id", "po_date", "promised_date", "receipt_date", "received_qty", "received_uom", "quality_status", "unit_cost", "currency_code", "receipt_variance_days")
    merge_delta(df, "gld_fact_purchase_receipt", ["receipt_id"])
    print("gld_fact_purchase_receipt written")

    # --- inlined from nb_gld_fact_sales_order_line.py ---
    from pyspark.sql import functions as F
    lines = valid_source("slv_sales_order_line")
    orders = valid_source("slv_sales_order").select("order_id", "customer_id", "order_timestamp", "requested_delivery_date", "sales_channel", "order_priority", "order_status")
    df = lines.join(orders, "order_id", "inner").withColumn("gross_sales_thb", F.col("ordered_qty") * F.col("unit_price_thb")).select("order_id", "order_line_no", "product_id", "fulfillment_location_id", "customer_id", "order_timestamp", "requested_delivery_date", "sales_channel", "order_priority", "order_status", "ordered_qty", "allocated_qty", "unit_price_thb", "gross_sales_thb", "promotion_flag", "line_status")
    merge_delta(df, "gld_fact_sales_order_line", ["order_id", "order_line_no"])
    print("gld_fact_sales_order_line written")

    # --- inlined from nb_gld_fact_shipment.py ---
    from pyspark.sql import functions as F
    shipments = valid_source("slv_shipment")
    orders = valid_source("slv_sales_order").select("order_id", "customer_id", "requested_delivery_date")
    df = shipments.join(orders, "order_id", "left").withColumn("dispatch_variance_hours", (F.col("actual_dispatch_timestamp").cast("long") - F.col("planned_dispatch_timestamp").cast("long")) / F.lit(3600.0)).select("shipment_id", "order_id", "customer_id", "carrier_id", "route_id", "planned_dispatch_timestamp", "actual_dispatch_timestamp", "requested_delivery_date", "shipment_status", "total_weight_value", "weight_uom", "dispatch_variance_hours")
    merge_delta(df, "gld_fact_shipment", ["shipment_id"])
    print("gld_fact_shipment written")

    # --- inlined from nb_gld_fact_delivery_event.py ---
    df = valid_source("slv_delivery_event").select("delivery_event_id", "shipment_id", "event_sequence", "event_type", "event_timestamp", "event_location", "proof_of_delivery_flag")
    merge_delta(df, "gld_fact_delivery_event", ["delivery_event_id"])
    print("gld_fact_delivery_event written")

    # --- inlined from nb_gld_fact_logistics_cost.py ---
    df = valid_source("slv_logistics_cost").select("cost_id", "shipment_id", "order_id", "cost_component", "amount", "currency_code", "posting_date")
    merge_delta(df, "gld_fact_logistics_cost", ["cost_id"])
    print("gld_fact_logistics_cost written")

    # --- inlined from nb_gld_fact_disruption_event.py ---
    df = valid_source("slv_disruption").select("disruption_id", "scope_type", "scope_id", "event_type", "start_date", "end_date", "severity", "delay_multiplier")
    merge_delta(df, "gld_fact_disruption_event", ["disruption_id"])
    print("gld_fact_disruption_event written")
    watermark_schema = T.StructType([T.StructField("process_name", T.StringType(), False), T.StructField("last_success_at", T.TimestampType(), False)])
    if not spark.catalog.tableExists("ops_incremental_watermark"):
        spark.createDataFrame([], watermark_schema).write.format("delta").saveAsTable("ops_incremental_watermark")
    spark.createDataFrame([(ORCHESTRATOR_NAME, datetime.now(timezone.utc))], watermark_schema).write.format("delta").mode("append").saveAsTable("ops_incremental_watermark")
    if not spark.catalog.tableExists(INPUT_WATERMARK_TABLE):
        spark.createDataFrame([], INPUT_WATERMARK_SCHEMA).write.format("delta").saveAsTable(INPUT_WATERMARK_TABLE)
    spark.createDataFrame([(ORCHESTRATOR_NAME, CURRENT_SILVER_WM)], INPUT_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(INPUT_WATERMARK_TABLE)
    append_log("SUCCESS", "All consolidated Gold fact transformations completed", stage_started, datetime.now(timezone.utc))
except Exception as error:
    append_log("FAILED", str(error)[:4000], stage_started, datetime.now(timezone.utc))
    raise
