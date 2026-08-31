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

`nb_incremental_gold_facts` now writes eight Gold facts, not five:
`gld_fact_demand_forecast`, `gld_fact_inventory_snapshot`,
`gld_fact_purchase_receipt`, `gld_fact_sales_order_line`, `gld_fact_shipment`,
plus `gld_fact_delivery_event`, `gld_fact_logistics_cost`, and
`gld_fact_disruption_event` (built in UAT, then promoted to and validated on
PROD with a live batch — see `CURRENT_STATUS.md` → "PROD promotion"). The
inputs for the last three (`slv_delivery_event`, `slv_logistics_cost`,
`slv_disruption`) already existed in Silver — they were simply never
registered in Gold's `DQ_TABLE_RULES` or written by Gold Facts. See
`CURRENT_STATUS.md` for the DQ-rule design decisions behind that change.

The `PROJECT_PLAN.md` canonical model also lists a ninth fact,
`fact_wms_activity`. It was never built: `wms_activity_events` is conformed
into Silver as `slv_wms_activity_event` but is not promoted to a Gold fact
and has no measures in the semantic model. So the as-built shape is:
18 Bronze entities → 18 Silver tables → 7 Gold dimensions + 8 Gold facts.

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

`required_field_nulls` and `duplicate_business_key` scan the full Silver
table, including quarantined rows — only `orphan_count` (referential
integrity) and Gold's own reads are restricted to `VALID` rows via
`valid_source()`. Keep `DQ_TABLE_RULES`'s required-columns list to true
identity/FK columns only; an attribute column that Silver's own quarantine
logic can legitimately null out (e.g. a timestamp that failed to parse) will
otherwise block Gold on old, already-quarantined data the first time that
table is added to the gate.

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
activity is not required. The live report (both UAT and PROD) has 8 pages:

- Executive Overview
- Sales & Demand
- Inventory & Fulfillment
- Transportation & Shipping
- Network & Cost-to-Serve
- Scenarios & Recommendations
- Data Quality Dashboard
- Data Health

The last three pages of the original 6-page plan (Transportation & Shipping,
Network & Cost-to-Serve, Scenarios & Recommendations) were built and
promoted to PROD in this project phase — see `CURRENT_STATUS.md` → "UAT
module build-out" and "PROD promotion" for what each page covers and how it
was validated.

The DQ page includes score, check counts, table impact, status distribution,
trend, check details, and slicers for time, status, and table.

### Editing the semantic model via TMDL View

Fabric's TMDL View (Preview) accepts scripted `createOrReplace` commands
against a live semantic model — useful for scripting table/relationship/
measure changes without Power BI Desktop. Two constraints learned building
the UAT model:

- **One `createOrReplace` command per Apply.** A script with multiple
  `createOrReplace` blocks fails with "Applying a script with multiple
  commands is not supported at this stage!" — split into separate Apply
  calls (or separate script tabs), one object at a time.
- **`createOrReplace table X` fully redefines the table.** A measure whose
  DAX formula references its own table by name (e.g.
  `COUNTROWS('gld_fact_delivery_event')`) fails to resolve if that table is
  being created for the first time in the same command — the validator
  checks formula references before the new table exists. Create the table
  (columns + partition, no self-referencing measures) first, confirm it
  applies, then re-apply the full definition including measures once the
  table already exists.
- New Direct Lake tables added this way are metadata-only until the semantic
  model is refreshed — DAX queries against them fail with `Cannot find table
  '<name>'` until then, even though the table shows in the model's Data
  pane. Trigger a refresh (Power BI REST API
  `POST .../datasets/{id}/refreshes`, or any normal pipeline run) before
  relying on the new tables in DAX or visuals.

## Source files

- UAT generator: `src/generate_incremental_batch.py`
- PROD Function entry: `azure_function/event_driven/app/generate_batch/__init__.py`
- PROD batch helper: `azure_function/event_driven/app/event_batch.py`
- Active Fabric definitions: `notebooks/incremental_v2/`

The UAT and PROD wrappers are separate, but shared transformation behavior and
data contracts remain aligned.
