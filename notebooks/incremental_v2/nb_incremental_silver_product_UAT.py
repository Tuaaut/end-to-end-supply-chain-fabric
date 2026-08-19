# Fabric notebook source: nb_incremental_silver_product_UAT
# Product-only path derived from the diagnostic that passed in UAT.

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window


PRODUCT_PATH = "Files/bronze/erp_products"
SUPPLIER_PATH = "Files/bronze/erp_suppliers"
TARGET_TABLE = "slv_product"


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

suppliers = (
    spark.read.option("header", True).option("recursiveFileLookup", "true").csv(SUPPLIER_PATH)
    .select(canonical_text(F.col("supplier_id")).alias("_supplier_id"))
    .dropDuplicates()
)

products_raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .option("recursiveFileLookup", "true")
    .csv(PRODUCT_PATH)
    .withColumn("source_file_name", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)
raw_columns = [column for column in products_raw.columns if column not in {"source_file_name", "ingested_at"}]

products = (
    products_raw.dropDuplicates(raw_columns)
    .withColumn("product_id", canonical_text(F.col("product_id")))
    .withColumn("product_name", F.regexp_replace(clean_text(F.col("product_name")), r"\s+", " "))
    .withColumn("category_key", canonical_text(F.col("category")))
    .withColumn("subcategory_key", canonical_text(F.col("subcategory")))
    .withColumn("primary_supplier_id", canonical_text(F.col("primary_supplier_id")))
    .withColumn("source_weight_uom", canonical_text(F.col("weight_uom")))
    .withColumn("weight_value_decimal", clean_text(F.col("weight_value")).cast(T.DecimalType(12, 3)))
    .withColumn("case_pack_qty", clean_text(F.col("case_pack_qty")).cast("int"))
    .withColumn("unit_list_price_thb", clean_text(F.col("unit_list_price_thb")).cast(T.DecimalType(12, 2)))
    .withColumn("shelf_life_days", clean_text(F.col("shelf_life_days")).cast("int"))
    .withColumn(
        "is_active",
        F.when(canonical_text(F.col("active_flag")).isin("Y", "1", "TRUE"), True).when(
            canonical_text(F.col("active_flag")).isin("N", "0", "FALSE"), False
        ),
    )
    .withColumn(
        "source_updated_at",
        F.coalesce(
            F.to_timestamp("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp("source_updated_at", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("source_updated_at", "yyyy/MM/dd HH:mm:ss"),
        ),
    )
    .withColumn(
        "unit_weight_kg",
        F.when(F.col("source_weight_uom") == "KG", F.col("weight_value_decimal"))
        .when(F.col("source_weight_uom") == "G", F.col("weight_value_decimal") / 1000)
        .cast(T.DecimalType(12, 3)),
    )
)

latest_window = Window.partitionBy("product_id").orderBy(
    F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc()
)
products_latest = products.withColumn("_rn", F.row_number().over(latest_window)).filter(F.col("_rn") == 1).drop("_rn")

hierarchy_rows = [
    ("AMBIENT FOOD", "RICE & GRAINS"), ("AMBIENT FOOD", "CANNED FOOD"),
    ("AMBIENT FOOD", "SNACKS"), ("AMBIENT FOOD", "COOKING ESSENTIALS"),
    ("BEVERAGE", "WATER"), ("BEVERAGE", "JUICE"),
    ("BEVERAGE", "CARBONATED DRINKS"), ("BEVERAGE", "FUNCTIONAL DRINKS"),
    ("PERSONAL CARE", "HAIR CARE"), ("PERSONAL CARE", "SKIN CARE"),
    ("PERSONAL CARE", "ORAL CARE"), ("PERSONAL CARE", "BODY CARE"),
    ("HOUSEHOLD", "LAUNDRY"), ("HOUSEHOLD", "HOME CLEANING"),
    ("HOUSEHOLD", "KITCHEN CARE"), ("HOUSEHOLD", "PAPER PRODUCTS"),
    ("HEALTHCARE", "FIRST AID"), ("HEALTHCARE", "SUPPLEMENTS"),
    ("HEALTHCARE", "MEDICAL SUPPLIES"), ("HEALTHCARE", "WELLNESS"),
]
hierarchy = spark.createDataFrame(hierarchy_rows, "category_key string, subcategory_key string").withColumn(
    "_valid_hierarchy", F.lit(True)
)
checked = products_latest.join(hierarchy, ["category_key", "subcategory_key"], "left").join(
    suppliers, F.col("primary_supplier_id") == suppliers._supplier_id, "left"
)

dq_reason = F.when(F.col("product_id").isNull() | ~F.col("product_id").rlike(r"^SKU-[0-9]{4}$"), "INVALID_PRODUCT_ID")
dq_reason = F.when(dq_reason.isNull() & F.col("product_name").isNull(), "MISSING_PRODUCT_NAME").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & F.col("_valid_hierarchy").isNull(), "INVALID_CATEGORY_SUBCATEGORY_PAIR").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & F.col("_supplier_id").isNull(), "UNRESOLVED_SUPPLIER_ID").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & ~F.col("source_weight_uom").isin("KG", "G"), "INVALID_WEIGHT_UOM").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & (F.col("unit_weight_kg").isNull() | (F.col("unit_weight_kg") <= 0)), "NON_POSITIVE_WEIGHT").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & (F.col("case_pack_qty").isNull() | (F.col("case_pack_qty") <= 0)), "INVALID_CASE_PACK_QTY").otherwise(dq_reason)
dq_reason = F.when(dq_reason.isNull() & F.col("is_active").isNull(), "INVALID_ACTIVE_FLAG").otherwise(dq_reason)

slv_product = (
    checked.withColumn("dq_reason", dq_reason)
    .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("QUARANTINED"))
    .select(
        "product_id", "product_name",
        F.initcap(F.lower("category_key")).alias("category_name"),
        F.initcap(F.lower("subcategory_key")).alias("subcategory_name"),
        "primary_supplier_id", "unit_weight_kg", "source_weight_uom", "case_pack_qty",
        "unit_list_price_thb", "shelf_life_days", "is_active", "source_updated_at",
        "source_file_name", "ingested_at",
        F.sha2(
            F.concat_ws(
                "||", "product_id", "product_name", "category_key", "subcategory_key",
                "primary_supplier_id", F.col("unit_weight_kg").cast("string"),
                F.col("case_pack_qty").cast("string"), F.col("unit_list_price_thb").cast("string"),
                F.col("is_active").cast("string"),
            ), 256,
        ).alias("row_hash"),
        "dq_status", "dq_reason",
    )
)

if spark.catalog.tableExists(TARGET_TABLE):
    cutoff = spark.table(TARGET_TABLE).agg(F.max("source_updated_at").alias("cutoff")).first()["cutoff"]
    incoming = slv_product if cutoff is None else slv_product.filter(F.col("source_updated_at") > F.lit(cutoff))
    if incoming.limit(1).count() == 0:
        print(f"NO_CHANGES {TARGET_TABLE} at {datetime.now(timezone.utc).isoformat()}")
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("t").merge(
            incoming.dropDuplicates(["product_id"]).alias("s"),
            "t.`product_id` <=> s.`product_id`",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"MERGED {TARGET_TABLE}")
else:
    slv_product.dropDuplicates(["product_id"]).coalesce(1).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TARGET_TABLE)
    print(f"CREATED {TARGET_TABLE}")
