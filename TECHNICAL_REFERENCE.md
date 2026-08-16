# Technical reference

## Active pipeline components

Both environments use the same seven-stage transformation design:

1. `nb_incremental_silver_supplier`
2. `nb_incremental_silver_product`
3. `nb_incremental_silver_commercial`
4. `nb_incremental_silver_fulfillment`
5. `nb_incremental_silver_planning_ops`
6. `nb_incremental_gold_dimensions`
7. `nb_incremental_gold_facts`

UAT items carry the `_UAT` suffix in Fabric. PROD uses the event-driven
pipeline with environment-specific bindings. Semantic Model refresh follows
Gold Facts.

## Incremental processing

Silver reads the 18 Bronze entity folders recursively, parses and normalizes
the input, applies data-quality status, and merges by natural business keys.
The file watermark and `source_updated_at` determine which records are new or
updated. A business date alone does not advance processing.

The active PROD volume profile is approximately 1,600 orders and 4,800 order
lines per batch. UAT uses the linked local incremental generator and its
manifest-driven batch layout.

## Data quality

The centralized Gold Dimensions gate writes to `ops_data_quality_summary`.

Blocking checks:

- required table and schema contract;
- duplicate business keys;
- required field nulls;
- orphan foreign keys across conformed Silver tables.

Operational warning:

- a row-count decrease greater than 30% from the accepted baseline.

The warning remains visible, and an accepted run becomes the next row-count
baseline. Gold reads `VALID` Silver rows only. DQ score and check detail are
available in the report DQ page.

## Date and key conventions

- Order IDs: `SO-########`.
- `source_updated_at`: UTC, Spark-compatible
  `yyyy-MM-dd'T'HH:mm:ss` ingestion timestamp.
- `gld_dim_date`: range derived from relevant Silver fact dates, including
  demand, inventory, requested delivery, receipt, and planned dispatch.
- Gold dimensions and facts use conformed keys from Silver and preserve the
  source-to-report lineage.

## Semantic model and report

The semantic models use Direct Lake against the environment-specific Lakehouse.
The report depends on the refreshed Semantic Model; a separate report refresh
activity is not required. Report pages are:

- Executive Control Tower
- Demand and Inventory
- Warehouse and Fulfillment
- Transportation and Shipping
- Network and Cost-to-Serve
- Scenarios and Recommendations

The DQ page includes score, check counts, table impact, status distribution,
trend, check details, and slicers for time, status, and table.

## Source files

- UAT generator: `src/generate_incremental_batch.py`
- PROD Function entry: `azure_function/event_driven/app/generate_batch/__init__.py`
- PROD batch helper: `azure_function/event_driven/app/event_batch.py`
- Active Fabric definitions: `notebooks/incremental_v2/`

The UAT and PROD wrappers are separate, but shared transformation behavior and
data contracts remain aligned.
