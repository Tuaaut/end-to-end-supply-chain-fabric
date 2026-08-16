# Fabric notebook source: nb_incremental_silver_commercial_UAT
# Attach the Lakehouse for the target environment before running.
# Single Spark session: all Silver transformations run in dependency order.

from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

pipeline_run_id = ""
ORCHESTRATOR_NAME = "nb_incremental_silver_commercial_v2"
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
append_log("STARTED", "Starting Silver commercial business group", stage_started)
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
    # --- inlined from nb_customer_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_customer_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    CUSTOMER_PATH = bronze_entity_path("crm_customers")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    customers_raw = (
        read_bronze_csv("crm_customers")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in customers_raw.columns if column not in {"source_file_name", "ingested_at"}]

    customers = (
        customers_raw.dropDuplicates(raw_columns)
        .withColumn("customer_id", canonical_text(F.col("customer_id")))
        .withColumn("customer_name", clean_text(F.col("customer_name")))
        .withColumn("region", F.initcap(F.lower(clean_text(F.col("region")))))
        .withColumn("channel", F.initcap(F.lower(clean_text(F.col("channel")))))
        .withColumn("service_tier", F.initcap(F.lower(clean_text(F.col("service_tier")))))
        .withColumn("latitude", clean_text(F.col("latitude")).cast(T.DecimalType(9, 5)))
        .withColumn("longitude", clean_text(F.col("longitude")).cast(T.DecimalType(9, 5)))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("customer_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_customer = (
        customers.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn(
            "dq_reason",
            F.when(F.col("customer_id").isNull() | ~F.col("customer_id").rlike(r"^CUS-[0-9]{5}$"), "INVALID_CUSTOMER_ID")
            .when(F.col("customer_name").isNull(), "MISSING_CUSTOMER_NAME")
            .when(F.col("region").isNull(), "MISSING_REGION")
            .when(F.col("channel").isNull(), "MISSING_CHANNEL")
            .when(F.col("service_tier").isNull(), "MISSING_SERVICE_TIER"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "customer_id", "customer_name", "region", "channel", "service_tier",
            "latitude", "longitude", "source_updated_at", "source_file_name", "ingested_at",
            "dq_status", "dq_reason",
        )
    )

    # One write only: keep Trial jobs small and independently verifiable.
    merge_delta(slv_customer.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_customer", ["customer_id"])
    print("slv_customer write submitted successfully")

    # --- inlined from nb_location_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_location_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    LOCATION_PATH = bronze_entity_path("erp_locations")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    locations_raw = (
        read_bronze_csv("erp_locations")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in locations_raw.columns if column not in {"source_file_name", "ingested_at"}]

    locations = (
        locations_raw.dropDuplicates(raw_columns)
        .withColumn("location_id", canonical_text(F.col("location_id")))
        .withColumn("location_name", clean_text(F.col("location_name")))
        .withColumn("region", F.initcap(F.lower(clean_text(F.col("region")))))
        .withColumn("country_code", canonical_text(F.col("country_code")))
        .withColumn("daily_capacity_units", clean_text(F.col("daily_capacity_units")).cast(T.IntegerType()))
        .withColumn("storage_capacity_units", clean_text(F.col("storage_capacity_units")).cast(T.IntegerType()))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("location_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_location = (
        locations.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn(
            "dq_reason",
            F.when(F.col("location_id").isNull() | ~F.col("location_id").rlike(r"^DC-[A-Z]{3}$"), "INVALID_LOCATION_ID")
            .when(F.col("location_name").isNull(), "MISSING_LOCATION_NAME")
            .when(F.col("daily_capacity_units").isNull() | (F.col("daily_capacity_units") <= 0), "INVALID_DAILY_CAPACITY")
            .when(F.col("storage_capacity_units").isNull() | (F.col("storage_capacity_units") <= 0), "INVALID_STORAGE_CAPACITY"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "location_id", "location_name", "region", "country_code", "daily_capacity_units",
            "storage_capacity_units", "source_updated_at", "source_file_name", "ingested_at",
            "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_location.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_location", ["location_id"])
    print("slv_location write submitted successfully")

    # --- inlined from nb_carrier_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_carrier_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    CARRIER_PATH = bronze_entity_path("tms_carriers")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    carriers_raw = (
        read_bronze_csv("tms_carriers")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in carriers_raw.columns if column not in {"source_file_name", "ingested_at"}]

    carriers = (
        carriers_raw.dropDuplicates(raw_columns)
        .withColumn("carrier_id", canonical_text(F.col("carrier_id")))
        .withColumn("carrier_name", clean_text(F.col("carrier_name")))
        .withColumn("transport_mode", F.initcap(F.lower(clean_text(F.col("mode")))))
        .withColumn("base_rate_thb_per_kg", clean_text(F.col("base_rate_thb_per_kg")).cast(T.DecimalType(12, 2)))
        .withColumn("fuel_surcharge_pct", clean_text(F.col("fuel_surcharge_pct")).cast(T.DecimalType(8, 4)))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("carrier_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_carrier = (
        carriers.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn(
            "dq_reason",
            F.when(F.col("carrier_id").isNull() | ~F.col("carrier_id").rlike(r"^CAR-[0-9]{2}$"), "INVALID_CARRIER_ID")
            .when(F.col("carrier_name").isNull(), "MISSING_CARRIER_NAME")
            .when(F.col("base_rate_thb_per_kg").isNull() | (F.col("base_rate_thb_per_kg") <= 0), "INVALID_BASE_RATE")
            .when(F.col("fuel_surcharge_pct").isNull() | (F.col("fuel_surcharge_pct") < 0), "INVALID_FUEL_SURCHARGE"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "carrier_id", "carrier_name", "transport_mode", "base_rate_thb_per_kg",
            "fuel_surcharge_pct", "source_updated_at", "source_file_name", "ingested_at",
            "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_carrier.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_carrier", ["carrier_id"])
    print("slv_carrier write submitted successfully")

    # --- inlined from nb_route_bronze_to_silver_mvp.py ---
    # Fabric notebook source: nb_route_bronze_to_silver_mvp
    # Attach Lakehouse: lh_supply_chain_dev before running.

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window


    ROUTE_PATH = bronze_entity_path("tms_routes")


    def clean_text(column):
        return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


    def canonical_text(column):
        return F.upper(clean_text(column))


    locations = spark.table("slv_location").select(F.col("location_id").alias("_location_id"))
    routes_raw = (
        read_bronze_csv("tms_routes")
        .withColumn("source_file_name", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )
    raw_columns = [column for column in routes_raw.columns if column not in {"source_file_name", "ingested_at"}]

    routes = (
        routes_raw.dropDuplicates(raw_columns)
        .withColumn("route_id", canonical_text(F.col("route_id")))
        .withColumn("origin_location_id", canonical_text(F.col("origin_location_id")))
        .withColumn("destination_region", F.initcap(F.lower(clean_text(F.col("destination_region")))))
        .withColumn("distance_km", clean_text(F.col("distance_km")).cast(T.DecimalType(12, 2)))
        .withColumn("standard_transit_days", clean_text(F.col("standard_transit_days")).cast(T.IntegerType()))
        .withColumn("toll_cost_thb", clean_text(F.col("toll_cost_thb")).cast(T.DecimalType(12, 2)))
        .withColumn("source_updated_at", F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"))
    )

    latest_window = Window.partitionBy("route_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
    )

    slv_route = (
        routes.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(locations, F.col("origin_location_id") == locations._location_id, "left")
        .withColumn(
            "dq_reason",
            F.when(F.col("route_id").isNull(), "INVALID_ROUTE_ID")
            .when(F.col("_location_id").isNull(), "UNRESOLVED_ORIGIN_LOCATION")
            .when(F.col("destination_region").isNull(), "MISSING_DESTINATION_REGION")
            .when(F.col("distance_km").isNull() | (F.col("distance_km") <= 0), "INVALID_DISTANCE")
            .when(F.col("standard_transit_days").isNull() | (F.col("standard_transit_days") <= 0), "INVALID_TRANSIT_DAYS")
            .when(F.col("toll_cost_thb").isNull() | (F.col("toll_cost_thb") < 0), "INVALID_TOLL_COST"),
        )
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
        .select(
            "route_id", "origin_location_id", "destination_region", "distance_km",
            "standard_transit_days", "toll_cost_thb", "source_updated_at", "source_file_name",
            "ingested_at", "dq_status", "dq_reason",
        )
    )

    merge_delta(slv_route.filter(F.col("source_updated_at") > F.lit(SOURCE_CUTOFF)), "slv_route", ["route_id"])
    print("slv_route write submitted successfully")

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
