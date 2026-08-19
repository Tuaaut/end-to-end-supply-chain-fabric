# Fabric notebook source: nb_incremental_silver_supplier_UAT
# One target per Spark session to stay within the Fabric Trial circuit breaker.

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_PATH = "Files/bronze/erp_suppliers"
TARGET_TABLE = "slv_supplier"


def clean_text(column):
    return F.when(F.trim(column) == "", None).otherwise(F.trim(column))


def canonical_text(column):
    return F.upper(clean_text(column))


def delta_target_committed(table_name):
    log_path = f"Tables/{table_name}/_delta_log"
    try:
        fs = notebookutils.fs
    except NameError:
        fs = mssparkutils.fs
    if not fs.exists(log_path):
        return False
    return any(str(getattr(entry, "name", getattr(entry, "path", entry))).endswith(".json") for entry in fs.ls(log_path))


if not delta_target_committed(TARGET_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS `{TARGET_TABLE}`")

if spark.catalog.tableExists(TARGET_TABLE) and "dq_status" in spark.table(TARGET_TABLE).columns:
    # dq_status/dq_reason were added to this table after it already had rows.
    # Delta schema evolution leaves pre-existing rows null for a new column,
    # not 'VALID' -- backfill once so valid_source() doesn't silently drop
    # every historical supplier from Gold's joins.
    spark.sql(f"""
        UPDATE `{TARGET_TABLE}`
        SET dq_status = CASE WHEN supplier_id IS NOT NULL AND supplier_name IS NOT NULL THEN 'VALID' ELSE 'QUARANTINED' END,
            dq_reason = CASE
                WHEN supplier_id IS NULL THEN 'INVALID_SUPPLIER_ID'
                WHEN supplier_name IS NULL THEN 'MISSING_SUPPLIER_NAME'
                ELSE NULL
            END
        WHERE dq_status IS NULL
    """)

suppliers_raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .option("recursiveFileLookup", "true")
    .csv(BRONZE_PATH)
    .withColumn("source_file_name", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)
raw_columns = [column for column in suppliers_raw.columns if column not in {"source_file_name", "ingested_at"}]

suppliers = (
    suppliers_raw.dropDuplicates(raw_columns)
    .withColumn("supplier_id", canonical_text(F.col("supplier_id")))
    .withColumn("supplier_name", clean_text(F.col("supplier_name")))
    .withColumn("country_code", canonical_text(F.col("country_code")))
    .withColumn("contract_lead_time_days", clean_text(F.col("contract_lead_time_days")).cast("int"))
    .withColumn("payment_terms", F.upper(F.regexp_replace(clean_text(F.col("payment_terms")), r"\s+", "")))
    .withColumn(
        "source_updated_at",
        F.coalesce(
            F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp("source_updated_at", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("source_updated_at", "yyyy/MM/dd HH:mm:ss"),
        ),
    )
    .withColumn(
        "dq_reason",
        F.when(F.col("supplier_id").isNull(), "INVALID_SUPPLIER_ID")
        .when(F.col("supplier_name").isNull(), "MISSING_SUPPLIER_NAME"),
    )
    .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
)

latest_window = Window.partitionBy("supplier_id").orderBy(
    F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
)
slv_supplier = suppliers.withColumn("_rn", F.row_number().over(latest_window)).filter(F.col("_rn") == 1).drop("_rn")

if spark.catalog.tableExists(TARGET_TABLE):
    cutoff = spark.table(TARGET_TABLE).agg(F.max("source_updated_at").alias("cutoff")).first()["cutoff"]
    incoming = slv_supplier if cutoff is None else slv_supplier.filter(F.col("source_updated_at") > F.lit(cutoff))
    if incoming.limit(1).count() == 0:
        print(f"NO_CHANGES {TARGET_TABLE} at {datetime.now(timezone.utc).isoformat()}")
    else:
        # dq_status/dq_reason are new columns on an already-live table; allow
        # the merge to add them instead of failing on schema mismatch.
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        DeltaTable.forName(spark, TARGET_TABLE).alias("t").merge(
            incoming.dropDuplicates(["supplier_id"]).alias("s"),
            "t.`supplier_id` <=> s.`supplier_id`",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"MERGED {TARGET_TABLE}")
else:
    slv_supplier.dropDuplicates(["supplier_id"]).coalesce(1).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TARGET_TABLE)
    print(f"CREATED {TARGET_TABLE}")
