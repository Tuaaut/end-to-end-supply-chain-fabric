# Fabric notebook source: nb_incremental_silver_planning_ops_UAT
# Attach the Lakehouse for the target environment before running.
# Single Spark session: all Silver transformations run in dependency order.

from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

pipeline_run_id = ""
ORCHESTRATOR_NAME = "nb_incremental_silver_merge_v2"
LOG_PIPELINE_TABLE = "ops_pipeline_run_log"

PIPELINE_RUN_ID = pipeline_run_id or datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ")

SOURCE_CUTOFF = datetime(1970, 1, 1, tzinfo=timezone.utc)
if spark.catalog.tableExists("ops_incremental_watermark"):
    _wm = (spark.table("ops_incremental_watermark")
        .filter(F.col("process_name") == ORCHESTRATOR_NAME)
        .orderBy(F.col("last_success_at").desc()).limit(1).collect())
    if _wm and _wm[0]["last_success_at"] is not None:
        SOURCE_CUTOFF = _wm[0]["last_success_at"]

FILE_WATERMARK_TABLE = "ops_incremental_file_watermark"
BRONZE_ROOT = "Files/bronze"
REQUIRED_SILVER_TABLES = [
    "slv_supplier", "slv_product", "slv_customer", "slv_location", "slv_carrier", "slv_route",
    "slv_sales_order", "slv_sales_order_line", "slv_inventory_snapshot", "slv_purchase_order",
    "slv_purchase_order_receipt", "slv_shipment", "slv_shipment_line", "slv_delivery_event",
    "slv_demand_forecast", "slv_disruption", "slv_logistics_cost", "slv_wms_activity_event",
]
FILE_WATERMARK_SCHEMA = T.StructType([
    T.StructField("process_name", T.StringType(), False),
    T.StructField("last_file_modified_at", T.TimestampType(), False),
])


def bronze_snapshot_modified_at():
    return (spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(BRONZE_ROOT)
        .agg(F.max("modificationTime").alias("m")).first()["m"])


def bronze_entity_path(entity_name):
    """Return an entity folder; recursive lookup supports YYYY/MM/DD partitions."""
    return f"{BRONZE_ROOT}/{entity_name}"


def read_bronze_csv(entity_name):
    """Read all CSV batches for an entity, including nested date folders."""
    return (spark.read.option("header", True)
        .option("recursiveFileLookup", "true")
        .csv(bronze_entity_path(entity_name)))

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


def delta_target_committed(table_name):
    """Return True/False for a readable path, or None if inspection failed."""
    log_path = f"Tables/{table_name}/_delta_log"
    try:
        fs = notebookutils.fs
    except NameError:
        try:
            fs = mssparkutils.fs
        except Exception:
            return None
    try:
        if not fs.exists(log_path):
            return False
        entries = fs.ls(log_path)
    except Exception:
        return None
    return any(str(getattr(entry, "name", getattr(entry, "path", entry))).endswith(".json") for entry in entries)


def merge_delta(df, table_name, keys):
    """Idempotent key-based upsert; initial load creates the Delta target."""
    print(f"START {table_name}")
    target = None
    # Recover safely from an interrupted initial write with no Delta commit.
    if delta_target_committed(table_name) is False:
        spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
    if spark.catalog.tableExists(table_name):
        try:
            target = DeltaTable.forName(spark, table_name)
        except Exception as error:
            # A cancelled bootstrap can leave a metastore registration whose
            # Delta directory was never committed. Treat that as an absent
            # target, not as a MERGE candidate.
            if "DELTA_TABLE_NOT_FOUND" not in str(error):
                raise
            spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
            print(f"RECOVERED stale catalog registration for {table_name}")
    if target is None:
        # Avoid localCheckpoint here: Fabric may recycle the executor holding
        # its non-durable block before Delta commits the table.
        prepared = df.dropDuplicates(keys).coalesce(1)
        (prepared.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(table_name))
        print(f"DONE {table_name} (initial write)")
        return
    source = df.dropDuplicates(keys)
    condition = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
    (target.alias("t").merge(source.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"DONE {table_name} (merge)")


def exit_success(message):
    append_log("SUCCESS", message, stage_started, datetime.now(timezone.utc))
    try:
        notebookutils.notebook.exit("SUCCESS")
    except NameError:
        mssparkutils.notebook.exit("SUCCESS")


stage_started = datetime.now(timezone.utc)
append_log("STARTED", "Starting Silver planning_ops business group", stage_started)
try:
    # Resolve bootstrap state with one catalog call. The former preflight made
    # more than twenty sequential tableExists/DROP calls before the first
    # Silver write and could exhaust the activity timeout on Fabric Trial.
    existing_table_names = {table.name for table in spark.catalog.listTables()}
    initial_load_incomplete = not set(REQUIRED_SILVER_TABLES).issubset(existing_table_names)
    current_file_modified_at = None
    previous_file_modified_at = None
    if initial_load_incomplete:
        # Reuse any valid partial targets through idempotent MERGE. Do not drop
        # and recreate completed work during bootstrap recovery.
        SOURCE_CUTOFF = datetime(1970, 1, 1, tzinfo=timezone.utc)
        print("BOOTSTRAP initial Silver load/resume")
    else:
        current_file_modified_at = bronze_snapshot_modified_at()
        _previous_file_wm = (spark.table(FILE_WATERMARK_TABLE)
            .filter(F.col("process_name") == ORCHESTRATOR_NAME)
            .orderBy(F.col("last_file_modified_at").desc()).limit(1).collect()
        ) if FILE_WATERMARK_TABLE in existing_table_names else []
        previous_file_modified_at = _previous_file_wm[0]["last_file_modified_at"] if _previous_file_wm else None
        if FILE_WATERMARK_TABLE not in existing_table_names and "ops_incremental_watermark" in existing_table_names:
            spark.createDataFrame([], FILE_WATERMARK_SCHEMA).write.format("delta").saveAsTable(FILE_WATERMARK_TABLE)
            spark.createDataFrame([(ORCHESTRATOR_NAME, current_file_modified_at)], FILE_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(FILE_WATERMARK_TABLE)
            append_log("SUCCESS", "Existing MERGE watermark bootstrapped; no Bronze file changes", stage_started, datetime.now(timezone.utc))
            try:
                notebookutils.notebook.exit("NO_CHANGES")
            except NameError:
                mssparkutils.notebook.exit("NO_CHANGES")
        if previous_file_modified_at is not None and current_file_modified_at <= previous_file_modified_at:
            append_log("SUCCESS", "No Bronze file changes; Silver MERGE skipped", stage_started, datetime.now(timezone.utc))
            try:
                notebookutils.notebook.exit("NO_CHANGES")
            except NameError:
                mssparkutils.notebook.exit("NO_CHANGES")
    # --- inlined from nb_demand_forecast_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_demand_forecast_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    FORECAST_PATH = bronze_entity_path("planning_demand_forecast")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    forecast_raw = (
        read_bronze_csv("planning_demand_forecast")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in forecast_raw.columns if column not in {"source_file_name", "ingested_at"}]

    forecast = (
        forecast_raw.dropDuplicates(raw_columns)
        .withColumn("demand_date", F.to_date("demand_date", "yyyy-MM-dd"))
        .withColumn("product_id", canonical_text(F.col("product_id")))
        .withColumn("location_id", canonical_text(F.col("location_id")))
        .withColumn("forecast_version", F.upper(clean_text(F.col("forecast_version"))))
        .withColumn("forecast_qty", clean_text(F.col("forecast_qty")).cast(T.IntegerType()))
        .withColumn("actual_demand_qty", clean_text(F.col("actual_demand_qty")).cast(T.IntegerType()))
        .withColumn("demand_uom", canonical_text(F.col("demand_uom")))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("demand_date", "product_id", "location_id", "forecast_version").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_demand_forecast = (
        forecast.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(products, F.col("product_id") == products._product_id, "left")
        .join(locations, F.col("location_id") == locations._location_id, "left")
        .withColumn(
            "dq_reason",
            F.when(F.col("demand_date").isNull(), "INVALID_DEMAND_DATE")
            .when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID")
            .when(F.col("_location_id").isNull(), "UNRESOLVED_LOCATION_ID")
            .when(F.col("forecast_version").isNull(), "MISSING_FORECAST_VERSION")
            .when(F.col("forecast_qty").isNull() | (F.col("forecast_qty") < 0), "INVALID_FORECAST_QTY")
            .when(F.col("actual_demand_qty").isNull() | (F.col("actual_demand_qty") < 0), "INVALID_ACTUAL_DEMAND_QTY")
            .when(~F.col("demand_uom").isin("EA", "EACH"), "INVALID_DEMAND_UOM"),
        )
        .withColumn("forecast_error_qty", F.col("actual_demand_qty") - F.col("forecast_qty"))
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "demand_date", "product_id", "location_id", "forecast_version", "forecast_qty",
            "actual_demand_qty", "forecast_error_qty", "demand_uom", "source_updated_at",
            "source_file_name", "ingested_at", "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_demand_forecast.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_demand_forecast", ["demand_date", "product_id", "location_id", "forecast_version"])
    print("slv_demand_forecast write submitted successfully")

    # --- inlined from nb_disruption_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_disruption_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("risk_disruptions")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))
    raw = read_bronze_csv("risk_disruptions").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("disruption_id", key(F.col("disruption_id"))).withColumn("scope_type", key(F.col("scope_type"))).withColumn("scope_id", clean(F.col("scope_id"))).withColumn("event_type", F.initcap(F.lower(clean(F.col("event_type"))))).withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd")).withColumn("end_date", F.to_date("end_date", "yyyy-MM-dd")).withColumn("severity", key(F.col("severity"))).withColumn("delay_multiplier", clean(F.col("delay_multiplier")).cast(T.DecimalType(8, 2))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("disruption_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_disruption = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").withColumn("dq_reason", F.when(F.col("disruption_id").isNull(), "INVALID_DISRUPTION_ID").when(~F.col("scope_type").isin("REGION", "SUPPLIER", "LOCATION", "ROUTE", "CARRIER"), "INVALID_SCOPE_TYPE").when(F.col("scope_id").isNull(), "MISSING_SCOPE_ID").when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE").when(F.col("start_date").isNull() | F.col("end_date").isNull() | (F.col("end_date") < F.col("start_date")), "INVALID_EVENT_DATES").when(~F.col("severity").isin("LOW", "MEDIUM", "HIGH", "CRITICAL"), "INVALID_SEVERITY").when(F.col("delay_multiplier").isNull() | (F.col("delay_multiplier") <= 0), "INVALID_DELAY_MULTIPLIER")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("disruption_id", "scope_type", "scope_id", "event_type", "start_date", "end_date", "severity", "delay_multiplier", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_disruption.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_disruption", ["disruption_id"])
    print("slv_disruption write submitted successfully")

    # --- inlined from nb_logistics_cost_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_logistics_cost_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("finance_logistics_costs")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))
    shipments = spark.table("slv_shipment").select(F.col("shipment_id").alias("_shipment_id"))
    orders = spark.table("slv_sales_order").select(F.col("order_id").alias("_order_id"))
    raw = read_bronze_csv("finance_logistics_costs").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("cost_id", key(F.col("cost_id"))).withColumn("shipment_id", key(F.col("shipment_id"))).withColumn("order_id", key(F.col("order_id"))).withColumn("cost_component", key(F.col("cost_component"))).withColumn("amount", clean(F.col("amount")).cast(T.DecimalType(14, 2))).withColumn("currency_code", key(F.col("currency_code"))).withColumn("posting_date", F.to_date("posting_date", "yyyy-MM-dd")).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("cost_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_logistics_cost = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(shipments, data.shipment_id == shipments._shipment_id, "left").join(orders, data.order_id == orders._order_id, "left").withColumn("dq_reason", F.when(F.col("cost_id").isNull(), "INVALID_COST_ID").when(F.col("_shipment_id").isNull(), "UNRESOLVED_SHIPMENT_ID").when(F.col("_order_id").isNull(), "UNRESOLVED_ORDER_ID").when(F.col("cost_component").isNull(), "MISSING_COST_COMPONENT").when(F.col("amount").isNull() | (F.col("amount") < 0), "INVALID_AMOUNT").when(~F.col("currency_code").isin("THB", "USD"), "INVALID_CURRENCY").when(F.col("posting_date").isNull(), "INVALID_POSTING_DATE")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("cost_id", "shipment_id", "order_id", "cost_component", "amount", "currency_code", "posting_date", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_logistics_cost.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_logistics_cost", ["cost_id"])
    print("slv_logistics_cost write submitted successfully")

    # --- inlined from nb_wms_activity_event_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_wms_activity_event_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("wms_activity_events")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))
    orders = spark.table("slv_sales_order").select(F.col("order_id").alias("_order_id"))
    shipments = spark.table("slv_shipment").select(F.col("shipment_id").alias("_shipment_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    raw = read_bronze_csv("wms_activity_events").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("wms_event_id", key(F.col("wms_event_id"))).withColumn("order_id", key(F.col("order_id"))).withColumn("shipment_id", key(F.col("shipment_id"))).withColumn("location_id", key(F.col("location_id"))).withColumn("event_type", key(F.col("event_type"))).withColumn("event_timestamp", F.coalesce(F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss"), F.to_timestamp("event_timestamp", "MM/dd/yyyy hh:mm a"))).withColumn("operator_shift", key(F.col("operator_shift"))).withColumn("quantity_processed", clean(F.col("quantity_processed")).cast(T.IntegerType())).withColumn("exception_code", key(F.col("exception_code"))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("wms_event_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_wms_activity_event = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(orders, data.order_id == orders._order_id, "left").join(shipments, data.shipment_id == shipments._shipment_id, "left").join(locations, data.location_id == locations._location_id, "left").withColumn("dq_reason", F.when(F.col("wms_event_id").isNull(), "INVALID_WMS_EVENT_ID").when(F.col("_order_id").isNull(), "UNRESOLVED_ORDER_ID").when(F.col("location_id").isNull() | F.col("_location_id").isNull(), "UNRESOLVED_LOCATION_ID").when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE").when(F.col("event_timestamp").isNull(), "INVALID_EVENT_TIMESTAMP").when(F.col("quantity_processed").isNull() | (F.col("quantity_processed") < 0), "INVALID_QUANTITY_PROCESSED").when(~F.col("operator_shift").isin("A", "B", "C"), "INVALID_OPERATOR_SHIFT")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("wms_event_id", "order_id", "shipment_id", "location_id", "event_type", "event_timestamp", "operator_shift", "quantity_processed", "exception_code", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_wms_activity_event.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_wms_activity_event", ["wms_event_id"])
    print("slv_wms_activity_event write submitted successfully")
    # v2 performance guard: persist one group-level watermark only.
    # Do not scan every Silver table at the end of each activity.
    watermark_schema = T.StructType([
        T.StructField("process_name", T.StringType(), False),
        T.StructField("last_success_at", T.TimestampType(), False),
    ])
    if not spark.catalog.tableExists("ops_incremental_watermark"):
        spark.createDataFrame([], watermark_schema).write.format("delta").saveAsTable("ops_incremental_watermark")
    completed_at = datetime.now(timezone.utc)
    spark.createDataFrame([(ORCHESTRATOR_NAME, completed_at)], watermark_schema).write.format("delta").mode("append").saveAsTable("ops_incremental_watermark")
    if not spark.catalog.tableExists(FILE_WATERMARK_TABLE):
        spark.createDataFrame([], FILE_WATERMARK_SCHEMA).write.format("delta").saveAsTable(FILE_WATERMARK_TABLE)
    if current_file_modified_at is None:
        current_file_modified_at = bronze_snapshot_modified_at()
    if current_file_modified_at is not None:
        spark.createDataFrame([(ORCHESTRATOR_NAME, current_file_modified_at)], FILE_WATERMARK_SCHEMA).write.format("delta").mode("append").saveAsTable(FILE_WATERMARK_TABLE)
    append_log("SUCCESS", f"Silver group completed; source_cutoff={SOURCE_CUTOFF.isoformat()}", stage_started, completed_at)
except Exception as error:
    append_log("FAILED", str(error)[:4000], stage_started, datetime.now(timezone.utc))
    raise
