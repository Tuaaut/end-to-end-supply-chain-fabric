# Fabric notebook source: nb_incremental_silver_fulfillment_UAT
# Attach the Lakehouse for the target environment before running.
# Single Spark session: all Silver transformations run in dependency order.

from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

pipeline_run_id = ""
ORCHESTRATOR_NAME = "nb_incremental_silver_fulfillment_v2"
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
append_log("STARTED", "Starting Silver fulfillment business group", stage_started)
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
    # --- inlined from nb_sales_order_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_sales_order_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window


    ORDER_PATH = bronze_entity_path("erp_sales_orders")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    customers = spark.table("slv_customer").select(F.col("customer_id").alias("_customer_id"))
    orders_raw = (
        read_bronze_csv("erp_sales_orders")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in orders_raw.columns if column not in {"source_file_name", "ingested_at"}]

    orders = (
        orders_raw.dropDuplicates(raw_columns)
        .withColumn("order_id", canonical_text(F.col("order_id")))
        .withColumn("customer_id", canonical_text(F.col("customer_id")))
        .withColumn(
            "order_timestamp",
            F.coalesce(
                F.to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp("order_timestamp", "MM/dd/yyyy hh:mm a"),
                F.to_timestamp("order_timestamp", "dd/MM/yyyy HH:mm"),
                F.to_timestamp("order_timestamp", "yyyy/MM/dd HH:mm:ss"),
            ),
        )
        .withColumn("requested_delivery_date", F.to_date("requested_delivery_date", "yyyy-MM-dd"))
        .withColumn("sales_channel", F.initcap(F.lower(clean_text(F.col("sales_channel")))))
        .withColumn("order_priority", F.upper(clean_text(F.col("order_priority"))))
        .withColumn("order_status", F.upper(clean_text(F.col("order_status"))))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_sales_order = (
        orders.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(customers, F.col("customer_id") == customers._customer_id, "left")
        .withColumn(
            "dq_reason",
            F.when(F.col("order_id").isNull() | ~F.col("order_id").rlike(r"^SO-[0-9]{8}$"), "INVALID_ORDER_ID")
            .when(F.col("_customer_id").isNull(), "UNRESOLVED_CUSTOMER_ID")
            .when(F.col("order_timestamp").isNull(), "INVALID_ORDER_TIMESTAMP")
            .when(F.col("requested_delivery_date").isNull(), "INVALID_REQUESTED_DELIVERY_DATE")
            .when(F.col("sales_channel").isNull(), "MISSING_SALES_CHANNEL")
            .when(~F.col("order_priority").isin("NORMAL", "EXPEDITE"), "INVALID_ORDER_PRIORITY")
            .when(~F.col("order_status").isin("RELEASED", "CANCELLED", "ON HOLD", "FULFILLED"), "INVALID_ORDER_STATUS"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "order_id", "customer_id", "order_timestamp", "requested_delivery_date",
            "sales_channel", "order_priority", "order_status", "source_updated_at",
            "source_file_name", "ingested_at", "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_sales_order.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_sales_order", ["order_id"])
    print("slv_sales_order write submitted successfully")

    # --- inlined from nb_sales_order_line_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_sales_order_line_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    ORDER_LINE_PATH = bronze_entity_path("erp_sales_order_lines")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    orders = spark.table("slv_sales_order").select(F.col("order_id").alias("_order_id"))
    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    lines_raw = (
        read_bronze_csv("erp_sales_order_lines")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in lines_raw.columns if column not in {"source_file_name", "ingested_at"}]

    lines = (
        lines_raw.dropDuplicates(raw_columns)
        .withColumn("order_id", canonical_text(F.col("order_id")))
        .withColumn("order_line_no", clean_text(F.col("order_line_no")).cast(T.IntegerType()))
        .withColumn("product_id", canonical_text(F.col("product_id")))
        .withColumn("fulfillment_location_id", canonical_text(F.col("fulfillment_location_id")))
        .withColumn("ordered_qty", clean_text(F.col("ordered_qty")).cast(T.IntegerType()))
        .withColumn("allocated_qty", clean_text(F.col("allocated_qty")).cast(T.IntegerType()))
        .withColumn("order_uom", canonical_text(F.col("order_uom")))
        .withColumn("unit_price_thb", clean_text(F.col("unit_price_thb")).cast(T.DecimalType(12, 2)))
        .withColumn("promotion_flag", F.when(canonical_text(F.col("promotion_flag")).isin("Y", "1", "TRUE"), True).when(canonical_text(F.col("promotion_flag")).isin("N", "0", "FALSE"), False))
        .withColumn("line_status", F.upper(clean_text(F.col("line_status"))))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("order_id", "order_line_no").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_sales_order_line = (
        lines.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(orders, F.col("order_id") == orders._order_id, "left")
        .join(products, F.col("product_id") == products._product_id, "left")
        .join(locations, F.col("fulfillment_location_id") == locations._location_id, "left")
        .withColumn(
            "dq_reason",
            F.when(F.col("_order_id").isNull(), "UNRESOLVED_ORDER_ID")
            .when(F.col("order_line_no").isNull() | (F.col("order_line_no") <= 0), "INVALID_ORDER_LINE_NO")
            .when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID")
            .when(F.col("_location_id").isNull(), "UNRESOLVED_FULFILLMENT_LOCATION")
            .when(F.col("ordered_qty").isNull() | (F.col("ordered_qty") <= 0), "INVALID_ORDERED_QTY")
            .when(F.col("allocated_qty").isNull() | (F.col("allocated_qty") < 0) | (F.col("allocated_qty") > F.col("ordered_qty")), "INVALID_ALLOCATED_QTY")
            .when(~F.col("order_uom").isin("EA", "EACH"), "INVALID_ORDER_UOM")
            .when(F.col("unit_price_thb").isNull() | (F.col("unit_price_thb") < 0), "INVALID_UNIT_PRICE")
            .when(F.col("promotion_flag").isNull(), "INVALID_PROMOTION_FLAG"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "order_id", "order_line_no", "product_id", "fulfillment_location_id", "ordered_qty",
            "allocated_qty", "order_uom", "unit_price_thb", "promotion_flag", "line_status",
            "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_sales_order_line.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_sales_order_line", ["order_id", "order_line_no"])
    print("slv_sales_order_line write submitted successfully")

    # --- inlined from nb_inventory_snapshot_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_inventory_snapshot_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    INVENTORY_PATH = bronze_entity_path("wms_inventory_snapshot")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    inventory_raw = (
        read_bronze_csv("wms_inventory_snapshot")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in inventory_raw.columns if column not in {"source_file_name", "ingested_at"}]

    inventory = (
        inventory_raw.dropDuplicates(raw_columns)
        .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
        .withColumn("product_id", canonical_text(F.col("product_id")))
        .withColumn("location_id", canonical_text(F.col("location_id")))
        .withColumn("on_hand_qty", clean_text(F.col("on_hand_qty")).cast(T.IntegerType()))
        .withColumn("reserved_qty", clean_text(F.col("reserved_qty")).cast(T.IntegerType()))
        .withColumn("safety_stock_qty", clean_text(F.col("safety_stock_qty")).cast(T.IntegerType()))
        .withColumn("inventory_uom", canonical_text(F.col("inventory_uom")))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("snapshot_date", "product_id", "location_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_inventory_snapshot = (
        inventory.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(products, F.col("product_id") == products._product_id, "left")
        .join(locations, F.col("location_id") == locations._location_id, "left")
        .withColumn(
            "dq_reason",
            F.when(F.col("snapshot_date").isNull(), "INVALID_SNAPSHOT_DATE")
            .when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID")
            .when(F.col("_location_id").isNull(), "UNRESOLVED_LOCATION_ID")
            .when(F.col("on_hand_qty").isNull() | (F.col("on_hand_qty") < 0), "INVALID_ON_HAND_QTY")
            .when(F.col("reserved_qty").isNull() | (F.col("reserved_qty") < 0) | (F.col("reserved_qty") > F.col("on_hand_qty")), "INVALID_RESERVED_QTY")
            .when(F.col("safety_stock_qty").isNull() | (F.col("safety_stock_qty") < 0), "INVALID_SAFETY_STOCK_QTY")
            .when(~F.col("inventory_uom").isin("EA", "EACH"), "INVALID_INVENTORY_UOM"),
        )
        .withColumn("available_qty", F.col("on_hand_qty") - F.col("reserved_qty"))
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "snapshot_date", "product_id", "location_id", "on_hand_qty", "reserved_qty",
            "available_qty", "safety_stock_qty", "inventory_uom", "source_updated_at",
            "source_file_name", "ingested_at", "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_inventory_snapshot.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_inventory_snapshot", ["snapshot_date", "product_id", "location_id"])
    print("slv_inventory_snapshot write submitted successfully")

    # --- inlined from nb_purchase_order_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_purchase_order_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("erp_purchase_orders")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))

    suppliers = spark.table("slv_supplier").select(F.col("supplier_id").alias("_supplier_id"))
    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    raw = read_bronze_csv("erp_purchase_orders").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("po_id", key(F.col("po_id"))).withColumn("supplier_id", key(F.col("supplier_id"))).withColumn("product_id", key(F.col("product_id"))).withColumn("destination_location_id", key(F.col("destination_location_id"))).withColumn("po_date", F.to_date("po_date", "yyyy-MM-dd")).withColumn("promised_date", F.to_date("promised_date", "yyyy-MM-dd")).withColumn("ordered_qty", clean(F.col("ordered_qty")).cast(T.IntegerType())).withColumn("purchase_uom", key(F.col("purchase_uom"))).withColumn("unit_cost", clean(F.col("unit_cost")).cast(T.DecimalType(14, 2))).withColumn("currency_code", key(F.col("currency_code"))).withColumn("po_status", key(F.col("po_status"))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("po_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_purchase_order = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(suppliers, data.supplier_id == suppliers._supplier_id, "left").join(products, data.product_id == products._product_id, "left").join(locations, data.destination_location_id == locations._location_id, "left").withColumn("dq_reason", F.when(F.col("po_id").isNull(), "INVALID_PO_ID").when(F.col("_supplier_id").isNull(), "UNRESOLVED_SUPPLIER_ID").when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID").when(F.col("_location_id").isNull(), "UNRESOLVED_DESTINATION_LOCATION").when(F.col("po_date").isNull() | F.col("promised_date").isNull() | (F.col("promised_date") < F.col("po_date")), "INVALID_PO_DATES").when(F.col("ordered_qty").isNull() | (F.col("ordered_qty") <= 0), "INVALID_ORDERED_QTY").when(~F.col("purchase_uom").isin("EA", "EACH"), "INVALID_PURCHASE_UOM").when(F.col("unit_cost").isNull() | (F.col("unit_cost") < 0), "INVALID_UNIT_COST").when(~F.col("currency_code").isin("THB", "USD"), "INVALID_CURRENCY")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("po_id", "supplier_id", "product_id", "destination_location_id", "po_date", "promised_date", "ordered_qty", "purchase_uom", "unit_cost", "currency_code", "po_status", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_purchase_order.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_purchase_order", ["po_id"])
    print("slv_purchase_order write submitted successfully")

    # --- inlined from nb_purchase_order_receipt_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_purchase_order_receipt_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("wms_purchase_order_receipts")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))
    orders = spark.table("slv_purchase_order").select(F.col("po_id").alias("_po_id"))
    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    raw = read_bronze_csv("wms_purchase_order_receipts").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("receipt_id", key(F.col("receipt_id"))).withColumn("po_id", key(F.col("po_id"))).withColumn("product_id", key(F.col("product_id"))).withColumn("location_id", key(F.col("location_id"))).withColumn("receipt_timestamp", F.to_timestamp("receipt_timestamp", "yyyy-MM-dd'T'HH:mm:ss")).withColumn("promised_date", F.to_date("promised_date", "yyyy-MM-dd")).withColumn("received_qty", clean(F.col("received_qty")).cast(T.IntegerType())).withColumn("received_uom", key(F.col("received_uom"))).withColumn("quality_status", key(F.col("quality_status"))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("receipt_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_purchase_order_receipt = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(orders, data.po_id == orders._po_id, "left").join(products, data.product_id == products._product_id, "left").join(locations, data.location_id == locations._location_id, "left").withColumn("dq_reason", F.when(F.col("receipt_id").isNull(), "INVALID_RECEIPT_ID").when(F.col("_po_id").isNull(), "UNRESOLVED_PO_ID").when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID").when(F.col("_location_id").isNull(), "UNRESOLVED_LOCATION_ID").when(F.col("receipt_timestamp").isNull() | F.col("promised_date").isNull(), "INVALID_RECEIPT_DATES").when(F.col("received_qty").isNull() | (F.col("received_qty") <= 0), "INVALID_RECEIVED_QTY").when(~F.col("received_uom").isin("EA", "EACH"), "INVALID_RECEIVED_UOM").when(~F.col("quality_status").isin("PASS", "FAIL", "HOLD"), "INVALID_QUALITY_STATUS")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("receipt_id", "po_id", "product_id", "location_id", "receipt_timestamp", "promised_date", "received_qty", "received_uom", "quality_status", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_purchase_order_receipt.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_purchase_order_receipt", ["receipt_id"])
    print("slv_purchase_order_receipt write submitted successfully")

    # --- inlined from nb_shipment_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_shipment_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("tms_shipments")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))

    orders = spark.table("slv_sales_order").select(F.col("order_id").alias("_order_id"))
    carriers = spark.table("slv_carrier").select(F.col("carrier_id").alias("_carrier_id"))
    routes = spark.table("slv_route").select(F.col("route_id").alias("_route_id"))
    raw = read_bronze_csv("tms_shipments").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("shipment_id", key(F.col("shipment_id"))).withColumn("order_id", key(F.col("order_id"))).withColumn("carrier_id", key(F.col("carrier_id"))).withColumn("route_id", key(F.col("route_id"))).withColumn("planned_dispatch_timestamp", F.to_timestamp("planned_dispatch_timestamp", "yyyy-MM-dd'T'HH:mm:ss")).withColumn("actual_dispatch_timestamp", F.to_timestamp("actual_dispatch_timestamp", "yyyy-MM-dd'T'HH:mm:ss")).withColumn("shipment_status", key(F.col("shipment_status"))).withColumn("total_weight_value", clean(F.col("total_weight_value")).cast(T.DecimalType(14, 3))).withColumn("weight_uom", key(F.col("weight_uom"))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("shipment_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_shipment = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(orders, data.order_id == orders._order_id, "left").join(carriers, data.carrier_id == carriers._carrier_id, "left").join(routes, data.route_id == routes._route_id, "left").withColumn("dq_reason", F.when(F.col("shipment_id").isNull(), "INVALID_SHIPMENT_ID").when(F.col("_order_id").isNull(), "UNRESOLVED_ORDER_ID").when(F.col("_carrier_id").isNull(), "UNRESOLVED_CARRIER_ID").when(F.col("_route_id").isNull(), "UNRESOLVED_ROUTE_ID").when(F.col("planned_dispatch_timestamp").isNull(), "INVALID_PLANNED_DISPATCH").when(F.col("total_weight_value").isNull() | (F.col("total_weight_value") < 0), "INVALID_TOTAL_WEIGHT").when(~F.col("weight_uom").isin("KG", "G"), "INVALID_WEIGHT_UOM")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("shipment_id", "order_id", "carrier_id", "route_id", "planned_dispatch_timestamp", "actual_dispatch_timestamp", "shipment_status", "total_weight_value", "weight_uom", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_shipment.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_shipment", ["shipment_id"])
    print("slv_shipment write submitted successfully")

    # --- inlined from nb_shipment_line_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_shipment_line_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("tms_shipment_lines")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))

    shipments = spark.table("slv_shipment").select(F.col("shipment_id").alias("_shipment_id"))
    orders = spark.table("slv_sales_order").select(F.col("order_id").alias("_order_id"))
    products = spark.table("slv_product").select(F.col("product_id").alias("_product_id"))
    raw = read_bronze_csv("tms_shipment_lines").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("shipment_id", key(F.col("shipment_id"))).withColumn("order_id", key(F.col("order_id"))).withColumn("order_line_no", clean(F.col("order_line_no")).cast(T.IntegerType())).withColumn("product_id", key(F.col("product_id"))).withColumn("shipped_qty", clean(F.col("shipped_qty")).cast(T.IntegerType())).withColumn("shipment_uom", key(F.col("shipment_uom"))).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("shipment_id", "order_id", "order_line_no").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_shipment_line = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(shipments, data.shipment_id == shipments._shipment_id, "left").join(orders, data.order_id == orders._order_id, "left").join(products, data.product_id == products._product_id, "left").withColumn("dq_reason", F.when(F.col("_shipment_id").isNull(), "UNRESOLVED_SHIPMENT_ID").when(F.col("_order_id").isNull(), "UNRESOLVED_ORDER_ID").when(F.col("order_line_no").isNull() | (F.col("order_line_no") <= 0), "INVALID_ORDER_LINE_NO").when(F.col("_product_id").isNull(), "UNRESOLVED_PRODUCT_ID").when(F.col("shipped_qty").isNull() | (F.col("shipped_qty") <= 0), "INVALID_SHIPPED_QTY").when(~F.col("shipment_uom").isin("EA", "EACH"), "INVALID_SHIPMENT_UOM")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("shipment_id", "order_id", "order_line_no", "product_id", "shipped_qty", "shipment_uom", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_shipment_line.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_shipment_line", ["shipment_id", "order_id", "order_line_no"])
    print("slv_shipment_line write submitted successfully")

    # --- inlined from nb_delivery_event_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_delivery_event_bronze_to_silver_mvp
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PATH = bronze_entity_path("tms_delivery_events")
    def clean(c): return F.when(F.trim(c) == "", None).otherwise(F.trim(c))
    def key(c): return F.upper(clean(c))
    shipments = spark.table("slv_shipment").select(F.col("shipment_id").alias("_shipment_id"))
    raw = read_bronze_csv("tms_delivery_events").withColumn("source_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())
    raw_cols = [c for c in raw.columns if c not in {"source_file_name", "ingested_at"}]
    data = (raw.dropDuplicates(raw_cols).withColumn("delivery_event_id", key(F.col("delivery_event_id"))).withColumn("shipment_id", key(F.col("shipment_id"))).withColumn("event_sequence", clean(F.col("event_sequence")).cast(T.IntegerType())).withColumn("event_type", key(F.col("event_type"))).withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss")).withColumn("event_location", F.initcap(F.lower(clean(F.col("event_location"))))).withColumn("proof_of_delivery_flag", F.when(key(F.col("proof_of_delivery_flag")).isin("Y", "1", "TRUE"), True).when(key(F.col("proof_of_delivery_flag")).isin("N", "0", "FALSE"), False)).withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss")))
    latest = Window.partitionBy("delivery_event_id").orderBy(F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc())
    slv_delivery_event = (data.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn").join(shipments, data.shipment_id == shipments._shipment_id, "left").withColumn("dq_reason", F.when(F.col("delivery_event_id").isNull(), "INVALID_DELIVERY_EVENT_ID").when(F.col("_shipment_id").isNull(), "UNRESOLVED_SHIPMENT_ID").when(F.col("event_sequence").isNull() | (F.col("event_sequence") <= 0), "INVALID_EVENT_SEQUENCE").when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE").when(F.col("event_timestamp").isNull(), "INVALID_EVENT_TIMESTAMP").when(F.col("proof_of_delivery_flag").isNull(), "INVALID_PROOF_OF_DELIVERY_FLAG")).withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED")).select("delivery_event_id", "shipment_id", "event_sequence", "event_type", "event_timestamp", "event_location", "proof_of_delivery_flag", "source_updated_at", "source_file_name", "ingested_at", "dq_status", "dq_reason"))
    merge_delta(slv_delivery_event.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_delivery_event", ["delivery_event_id"])
    print("slv_delivery_event write submitted successfully")

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
