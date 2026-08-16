# Operations

This runbook describes the current controlled UAT and PROD operating paths.
Both paths are manual during validation; schedules and event triggers remain
disabled.

## UAT: generate, validate, upload, run

UAT does not use the Azure Function.

1. Generate a new dated batch with a real change-set and a watermark newer than
   the live UAT Silver watermark:

   ```bash
   python3 src/generate_incremental_batch.py \
     --date YYYY/MM/DD \
     --source-updated-at YYYY-MM-DDTHH:MM:SS \
     --change-set \
     --output data/bronze_batches
   ```
2. Validate the manifest and all 18 CSV files. Confirm the product master
   contains every transaction product, keys are linked, and the timestamp is
   Spark-compatible.
3. Upload each entity to:

   ```text
   <UAT Lakehouse>/Files/bronze/<entity>/YYYY/MM/DD/<entity>.csv
   ```
4. Start the UAT pipeline once:

   ```bash
   fab job start SupplyChain-UAT.Workspace/pl_supply_chain_incremental_UAT.DataPipeline
   fab job run-status SupplyChain-UAT.Workspace/pl_supply_chain_incremental_UAT.DataPipeline --id <job-id>
   ```
5. Confirm terminal success for Silver, Gold Dimensions, Gold Facts, Semantic
   Model refresh, and the report.

The active UAT notebook order is supplier, product, commercial, fulfillment,
planning/operations, Gold Dimensions, and Gold Facts. Semantic Model refresh
follows Gold Facts.

## PROD: Function-to-pipeline E2E

PROD starts at the Azure Function, not with a manually started pipeline.

1. Build the package:

   ```bash
   ./azure_function/event_driven/scripts/build_package.sh
   ```
2. Deploy the generated package to `func-sc-event-PROD-0812`.
3. Invoke `/api/generate-batch` with a new ID and:

   ```json
   {
     "batch_id": "sc-prod-<unique-id>",
     "batch_date": "YYYY/MM/DD",
     "deliver": true,
     "invoke_pipeline": true
   }
   ```
4. Confirm the Function response reports `DELIVERED`, 18 entity files, and a
   `_READY.json` marker.
5. Monitor the submitted job:

   ```bash
   fab job run-status SupplyChain-PROD.Workspace/pl_supply_chain_event_PROD.DataPipeline --id <job-id>
   ```
6. Confirm terminal success through Silver, Gold Dimensions, Gold Facts,
   Semantic Model refresh, and the report.

The active PROD profile targets approximately 1,600 orders and 4,800 order
lines per batch. Generated order IDs must remain `SO-########`.

## DQ and watermark checks

- `source_updated_at` is the ingestion watermark, not the business date.
- A new batch must use a strictly newer UTC timestamp in
  `yyyy-MM-dd'T'HH:mm:ss` format.
- Product, customer, location, carrier, route, and supplier references must
  resolve before Gold interpretation.
- Duplicate keys, required nulls, schema violations, and referential-integrity
  failures block Gold.
- A row-count drop greater than 30% is an operational warning and updates the
  accepted baseline after the run; it does not disable the other DQ gates.

## Fabric deployment discipline

Use Fabric CLI first:

```bash
fab auth status
fab ls
fab ls <workspace>.Workspace
```

After each import, export the live item and verify its destination binding and
definition. Preserve default Lakehouse metadata. Keep environment-specific IDs
and connections in manifests; never store credentials.

## Safety rules

- Never reuse a delivered batch ID.
- Never run two controlled batches concurrently.
- Do not blind-retry a failed run; inspect the failure reason first.
- Do not enable schedules or event triggers during validation.
- Do not manually run the PROD pipeline as a substitute for the Function E2E
  path unless the scope is explicitly changed.
- UAT and PROD share transformation concepts but keep separate entry points,
  Lakehouses, pipelines, and workspace bindings.
