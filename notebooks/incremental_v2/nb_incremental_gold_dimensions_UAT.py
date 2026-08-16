# Fabric notebook source: nb_incremental_gold_dimensions_UAT
# Attach Lakehouse: lh_supply_chain_dev before running.
# Single Spark session: all Gold dimension transformations run in dependency order.

from datetime import datetime, timezone
from functools import reduce
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

pipeline_run_id = ""
ORCHESTRATOR_NAME = "nb_incremental_gold_dimensions_merge_v2"
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
    elif spark.catalog.tableExists("gld_dim_location"):
        spark.createDataFrame([], INPUT_WATERMARK_SCHEMA).write.format("delta").saveAsTable(INPUT_WATERMARK_TABLE)
        spark.createDataFrame([(ORCHESTRATOR_NAME, CURRENT_SILVER_WM)], INPUT_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(INPUT_WATERMARK_TABLE)
        return True
    return False


# Centralized DQ gate: runs after Silver and before either Gold block writes.
# Keep the rules here so the existing three-notebook pipeline remains unchanged.
DQ_SUMMARY_TABLE = "ops_data_quality_summary"
VOLUME_DROP_THRESHOLD = 0.30
QUARANTINE_WARNING_RATE = 0.05
DQ_SUMMARY_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("checked_at", T.TimestampType(), False),
    T.StructField("checked_date", T.DateType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("check_name", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("actual_value", T.DoubleType(), True),
    T.StructField("expected_value", T.DoubleType(), True),
    T.StructField("threshold_value", T.DoubleType(), True),
    T.StructField("message", T.StringType(), True),
])

# Keys and required fields mirror the Silver-to-Gold contract. Add a table in
# one place when a new Gold input is introduced.
DQ_TABLE_RULES = [
    ("slv_supplier", ["supplier_id"], ["supplier_id", "supplier_name"]),
    ("slv_product", ["product_id"], ["product_id", "product_name", "primary_supplier_id"]),
    ("slv_customer", ["customer_id"], ["customer_id", "customer_name"]),
    ("slv_location", ["location_id"], ["location_id", "location_name"]),
    ("slv_carrier", ["carrier_id"], ["carrier_id", "carrier_name"]),
    ("slv_route", ["route_id"], ["route_id"]),
    ("slv_demand_forecast", ["demand_date", "product_id", "location_id", "forecast_version"], ["demand_date", "product_id", "location_id"]),
    ("slv_inventory_snapshot", ["snapshot_date", "product_id", "location_id"], ["snapshot_date", "product_id", "location_id"]),
    ("slv_purchase_order", ["po_id"], ["po_id", "supplier_id", "product_id", "destination_location_id"]),
    ("slv_purchase_order_receipt", ["receipt_id"], ["receipt_id", "po_id", "product_id", "location_id"]),
    ("slv_sales_order", ["order_id"], ["order_id", "customer_id"]),
    ("slv_sales_order_line", ["order_id", "order_line_no"], ["order_id", "order_line_no", "product_id", "fulfillment_location_id"]),
    ("slv_shipment", ["shipment_id"], ["shipment_id", "order_id", "carrier_id", "route_id"]),
]

# (child table, child foreign key, parent table, parent key)
DQ_REFERENTIAL_RULES = [
    ("slv_product", "primary_supplier_id", "slv_supplier", "supplier_id"),
    ("slv_demand_forecast", "product_id", "slv_product", "product_id"),
    ("slv_demand_forecast", "location_id", "slv_location", "location_id"),
    ("slv_inventory_snapshot", "product_id", "slv_product", "product_id"),
    ("slv_inventory_snapshot", "location_id", "slv_location", "location_id"),
    ("slv_purchase_order", "supplier_id", "slv_supplier", "supplier_id"),
    ("slv_purchase_order", "product_id", "slv_product", "product_id"),
    ("slv_purchase_order", "destination_location_id", "slv_location", "location_id"),
    ("slv_purchase_order_receipt", "po_id", "slv_purchase_order", "po_id"),
    ("slv_purchase_order_receipt", "product_id", "slv_product", "product_id"),
    ("slv_purchase_order_receipt", "location_id", "slv_location", "location_id"),
    ("slv_sales_order", "customer_id", "slv_customer", "customer_id"),
    ("slv_sales_order_line", "order_id", "slv_sales_order", "order_id"),
    ("slv_sales_order_line", "product_id", "slv_product", "product_id"),
    ("slv_sales_order_line", "fulfillment_location_id", "slv_location", "location_id"),
    ("slv_shipment", "order_id", "slv_sales_order", "order_id"),
    ("slv_shipment", "carrier_id", "slv_carrier", "carrier_id"),
    ("slv_shipment", "route_id", "slv_route", "route_id"),
]


def append_dq_result(rows, table_name, check_name, severity, status, actual_value=None,
                     expected_value=None, threshold_value=None, message=None):
    checked_at = datetime.now(timezone.utc)
    rows.append((
        PIPELINE_RUN_ID, checked_at, checked_at.date(), table_name, check_name,
        severity, status, float(actual_value) if actual_value is not None else None,
        float(expected_value) if expected_value is not None else None,
        float(threshold_value) if threshold_value is not None else None, message,
    ))


def previous_baseline(table_name):
    if not spark.catalog.tableExists(DQ_SUMMARY_TABLE):
        return None
    previous = (spark.table(DQ_SUMMARY_TABLE)
        .filter((F.col("table_name") == table_name)
                & (F.col("check_name") == "row_count")
                & F.col("status").isin("PASS", "BASELINE", "WARNING"))
        .orderBy(F.col("checked_at").desc()).limit(1).collect())
    return previous[0]["actual_value"] if previous else None


def orphan_count(child_table, child_key, parent_table, parent_key):
    child = valid_source(child_table).select(F.col(child_key).alias("_dq_fk")).filter(F.col("_dq_fk").isNotNull()).dropDuplicates()
    parent = valid_source(parent_table).select(F.col(parent_key).alias("_dq_fk")).filter(F.col("_dq_fk").isNotNull()).dropDuplicates()
    return child.join(parent, "_dq_fk", "left_anti").count()


def run_centralized_dq_gate():
    dq_rows = []
    failures = []

    for table_name, keys, required_columns in DQ_TABLE_RULES:
        if not spark.catalog.tableExists(table_name):
            append_dq_result(dq_rows, table_name, "table_exists", "FAIL", "FAIL", message="Required Silver table does not exist")
            failures.append(f"{table_name}: missing table")
            continue

        source = spark.table(table_name)
        missing_columns = [c for c in set(keys + required_columns) if c not in source.columns]
        if missing_columns:
            append_dq_result(dq_rows, table_name, "schema_contract", "FAIL", "FAIL", message=f"Missing required columns: {', '.join(sorted(missing_columns))}")
            failures.append(f"{table_name}: schema contract")
            continue

        row_count = source.count()
        baseline = previous_baseline(table_name)
        volume_drop = baseline is not None and row_count < baseline * (1 - VOLUME_DROP_THRESHOLD)
        # A controlled cleanup/backfill can legitimately reset the retained
        # population. Keep this visible and use it as the next baseline, but
        # do not block Gold; key, null, duplicate and RI checks still block.
        row_status = "BASELINE" if baseline is None else ("WARNING" if volume_drop else "PASS")
        append_dq_result(
            dq_rows, table_name, "row_count", "WARNING" if volume_drop else "INFO", row_status, row_count, baseline,
            VOLUME_DROP_THRESHOLD, "Initial baseline recorded" if baseline is None else "Row count drop over 30% is an operational warning; accepted runs reset the next baseline",
        )

        duplicate_groups = source.groupBy(*keys).count().filter(F.col("count") > 1).count()
        duplicate_status = "PASS" if duplicate_groups == 0 else "FAIL"
        append_dq_result(dq_rows, table_name, "duplicate_business_key", "FAIL", duplicate_status, duplicate_groups, 0, 0, f"Key: {', '.join(keys)}")
        if duplicate_status == "FAIL":
            failures.append(f"{table_name}: duplicate business key")

        required_null_condition = reduce(lambda left, right: left | right, [F.col(c).isNull() for c in required_columns])
        required_null_rows = source.filter(required_null_condition).count()
        required_status = "PASS" if required_null_rows == 0 else "FAIL"
        append_dq_result(dq_rows, table_name, "required_field_nulls", "FAIL", required_status, required_null_rows, 0, 0, f"Fields: {', '.join(required_columns)}")
        if required_status == "FAIL":
            failures.append(f"{table_name}: required field nulls")

        if "dq_status" in source.columns:
            quarantined_rows = source.filter(F.col("dq_status") != "VALID").count()
            quarantine_rate = quarantined_rows / row_count if row_count else 0.0
            # Silver has already isolated these rows and Gold reads only VALID.
            # Keep the rate visible for operations, while hard-gating conditions
            # that could otherwise corrupt the Gold model (volume, keys, nulls, RI).
            quarantine_status = "WARNING" if quarantined_rows else "PASS"
            append_dq_result(
                dq_rows, table_name, "quarantined_rows",
                "WARNING" if quarantine_status == "WARNING" else "INFO", quarantine_status,
                quarantined_rows, 0, None,
                "Rows isolated in Silver quarantine; Gold reads VALID rows only",
            )
            append_dq_result(
                dq_rows, table_name, "quarantined_row_rate",
                "WARNING" if quarantine_status == "WARNING" else "INFO", quarantine_status,
                quarantine_rate, 0, QUARANTINE_WARNING_RATE,
                f"{quarantined_rows} quarantined rows out of {row_count}; Gold reads VALID rows only",
            )
        else:
            append_dq_result(dq_rows, table_name, "quarantined_row_rate", "WARNING", "SKIPPED", message="Legacy Silver table has no dq_status column")

    for child_table, child_key, parent_table, parent_key in DQ_REFERENTIAL_RULES:
        if not spark.catalog.tableExists(child_table) or not spark.catalog.tableExists(parent_table):
            append_dq_result(dq_rows, child_table, f"referential_integrity:{child_key}", "FAIL", "FAIL", message=f"Missing child or parent table: {parent_table}")
            failures.append(f"{child_table}: missing RI table")
            continue
        child_columns = spark.table(child_table).columns
        parent_columns = spark.table(parent_table).columns
        if child_key not in child_columns or parent_key not in parent_columns:
            append_dq_result(dq_rows, child_table, f"referential_integrity:{child_key}", "FAIL", "FAIL", message=f"Missing key for parent {parent_table}.{parent_key}")
            failures.append(f"{child_table}: RI schema")
            continue
        orphans = orphan_count(child_table, child_key, parent_table, parent_key)
        ri_status = "PASS" if orphans == 0 else "FAIL"
        append_dq_result(dq_rows, child_table, f"referential_integrity:{child_key}", "FAIL", ri_status, orphans, 0, 0, f"Parent: {parent_table}.{parent_key}")
        if ri_status == "FAIL":
            failures.append(f"{child_table}.{child_key}: orphan keys")

    total_checks = len(dq_rows)
    failed_checks = sum(1 for row in dq_rows if row[5] == "FAIL")
    dq_score = 100.0 if total_checks == 0 else round(100.0 * (total_checks - failed_checks) / total_checks, 2)
    append_dq_result(dq_rows, "__pipeline__", "dq_score", "INFO", "PASS" if not failures else "FAIL", dq_score, 100, 100, f"{failed_checks} failed checks out of {total_checks}")

    spark.createDataFrame(dq_rows, DQ_SUMMARY_SCHEMA).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(DQ_SUMMARY_TABLE)
    spark.sql(f"UPDATE {DQ_SUMMARY_TABLE} SET checked_date = CAST(checked_at AS DATE) WHERE checked_date IS NULL")
    if failures:
        raise ValueError("Centralized DQ gate failed; Gold was blocked. " + "; ".join(failures[:10]))
    print(f"Centralized DQ gate passed with score {dq_score}.")


stage_started = datetime.now(timezone.utc)
append_log("STARTED", "Starting consolidated Gold dimension transformations", stage_started)
try:
    if exit_if_no_new_silver():
        # A no-op data load is still a useful DQ control point. It verifies the
        # current Silver state while preserving the existing Gold fast path.
        run_centralized_dq_gate()
        append_log("SUCCESS", "No new Silver watermark; centralized DQ gate passed and Gold dimensions MERGE skipped", stage_started, datetime.now(timezone.utc))
        try:
            notebookutils.notebook.exit("NO_CHANGES")
        except NameError:
            mssparkutils.notebook.exit("NO_CHANGES")
    run_centralized_dq_gate()
    # --- inlined from nb_gld_dim_date.py ---
    from pyspark.sql import functions as F
    # Build the calendar from every business date used by the Silver facts.
    # This keeps fact-to-date relationships valid when event batches use
    # different dates for demand, delivery, receipt, or dispatch.
    date_columns = {
        "slv_demand_forecast": "demand_date",
        "slv_inventory_snapshot": "snapshot_date",
        "slv_sales_order": "requested_delivery_date",
        "slv_purchase_order_receipt": "receipt_timestamp",
        "slv_shipment": "planned_dispatch_timestamp",
    }
    source_frames = []
    for table_name, column_name in date_columns.items():
        source = valid_source(table_name)
        if column_name in source.columns:
            source_frames.append(source.select(F.to_date(F.col(column_name)).alias("d")))
    date_sources = source_frames[0]
    for source in source_frames[1:]:
        date_sources = date_sources.unionByName(source)
    date_sources = date_sources.filter(F.col("d").isNotNull())
    bounds = date_sources.agg(F.min("d").alias("min_date"), F.max("d").alias("max_date")).first()
    if bounds["min_date"] is None or bounds["max_date"] is None:
        date_schema = T.StructType([
            T.StructField("date_key", T.DateType(), True), T.StructField("year", T.IntegerType(), True),
            T.StructField("quarter", T.IntegerType(), True), T.StructField("month_number", T.IntegerType(), True),
            T.StructField("month_name", T.StringType(), True), T.StructField("week_of_year", T.IntegerType(), True),
            T.StructField("day_of_month", T.IntegerType(), True), T.StructField("day_name", T.StringType(), True),
        ])
        df = spark.createDataFrame([], date_schema)
    else:
        df = spark.sql(f"SELECT explode(sequence(to_date('{bounds.min_date}'), to_date('{bounds.max_date}'), interval 1 day)) AS date_key").select("date_key", F.year("date_key").alias("year"), F.quarter("date_key").alias("quarter"), F.month("date_key").alias("month_number"), F.date_format("date_key", "MMMM").alias("month_name"), F.weekofyear("date_key").alias("week_of_year"), F.dayofmonth("date_key").alias("day_of_month"), F.date_format("date_key", "EEEE").alias("day_name"))
    merge_delta(df, "gld_dim_date", ["date_key"])
    print("gld_dim_date written")

    # --- inlined from nb_gld_dim_location.py ---
    df = valid_source("slv_location").select("location_id", "location_name", "region", "country_code", "storage_capacity_units")
    merge_delta(df, "gld_dim_location", ["location_id"])
    print("gld_dim_location written")

    # --- inlined from nb_gld_dim_product.py ---
    from pyspark.sql import functions as F
    df = valid_source("slv_product").select("product_id", "product_name", "category_name", "subcategory_name", "primary_supplier_id", "unit_weight_kg", "unit_list_price_thb", "shelf_life_days", "is_active")
    merge_delta(df, "gld_dim_product", ["product_id"])
    print("gld_dim_product written")

    # --- inlined from nb_gld_dim_carrier.py ---
    df = valid_source("slv_carrier").select("carrier_id", "carrier_name", "transport_mode", "base_rate_thb_per_kg", "fuel_surcharge_pct")
    merge_delta(df, "gld_dim_carrier", ["carrier_id"])
    print("gld_dim_carrier written")

    # --- inlined from nb_gld_dim_customer.py ---
    df = valid_source("slv_customer").select("customer_id", "customer_name", "region", "channel", "service_tier", "latitude", "longitude")
    merge_delta(df, "gld_dim_customer", ["customer_id"])
    print("gld_dim_customer written")

    # --- inlined from nb_gld_dim_route.py ---
    df = valid_source("slv_route").select("route_id", "origin_location_id", "destination_region", "distance_km", "standard_transit_days", "toll_cost_thb")
    merge_delta(df, "gld_dim_route", ["route_id"])
    print("gld_dim_route written")

    # --- inlined from nb_gld_dim_supplier.py ---
    df = valid_source("slv_supplier").select("supplier_id", "supplier_name", "country_code", "payment_terms", F.col("active_flag").alias("is_active"))
    merge_delta(df, "gld_dim_supplier", ["supplier_id"])
    print("gld_dim_supplier written")
    watermark_schema = T.StructType([T.StructField("process_name", T.StringType(), False), T.StructField("last_success_at", T.TimestampType(), False)])
    if not spark.catalog.tableExists("ops_incremental_watermark"):
        spark.createDataFrame([], watermark_schema).write.format("delta").saveAsTable("ops_incremental_watermark")
    spark.createDataFrame([(ORCHESTRATOR_NAME, datetime.now(timezone.utc))], watermark_schema).write.format("delta").mode("append").saveAsTable("ops_incremental_watermark")
    if not spark.catalog.tableExists(INPUT_WATERMARK_TABLE):
        spark.createDataFrame([], INPUT_WATERMARK_SCHEMA).write.format("delta").saveAsTable(INPUT_WATERMARK_TABLE)
    spark.createDataFrame([(ORCHESTRATOR_NAME, CURRENT_SILVER_WM)], INPUT_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(INPUT_WATERMARK_TABLE)
    append_log("SUCCESS", "All consolidated Gold dimension transformations completed", stage_started, datetime.now(timezone.utc))
except Exception as error:
    append_log("FAILED", str(error)[:4000], stage_started, datetime.now(timezone.utc))
    raise
