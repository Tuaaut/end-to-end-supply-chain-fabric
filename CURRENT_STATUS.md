# Current status

Last updated: 2026-08-17 (Bangkok)

## Release status

The controlled UAT and PROD end-to-end paths are currently validated. The
project is in a stable manual-validation state with schedules and event
triggers disabled.

## PROD

- Workspace: `SupplyChain-PROD`
- Lakehouse: `lh_supply_chain_PROD`
- Pipeline: `pl_supply_chain_event_PROD`
- Function App: `func-sc-event-PROD-0812`
- Function route: `/api/generate-batch`
- Latest validated batch: `sc-prod-volume-e2e-20260816-06`
- Latest pipeline job: `85ffed1a-ec34-469d-a64b-216f708a5993`
- Validated path: Function → Bronze → Silver → Gold Dimensions → Gold Facts →
  Semantic Model → Report
- Volume profile: approximately 1,600 orders and 4,800 order lines per batch
- Order key contract: `SO-########`
- Event schedule/trigger: disabled

The PROD Function writes 18 linked entity files and a `_READY.json` marker to
OneLake, then submits the PROD Fabric pipeline. The Function package is built
from `azure_function/event_driven/build_output/` after running the package
build script.

## UAT

- Workspace: `SupplyChain-UAT`
- Lakehouse: `lh_supply_chain_UAT`
- Pipeline: `pl_supply_chain_incremental_UAT`
- Latest validated batch date: `2026/08/21`
- Latest ingestion watermark: `2026-08-21T06:00:00`
- Latest pipeline job: `114bb5c9-c84b-43ef-845d-f691a6b43b8d`
- Entry point: local generator and manual Bronze upload
- Azure Function: not used
- Schedule/trigger: disabled

The latest UAT validation used 18 linked CSV files and a genuine change-set.
The UAT Gold notebook was imported with the same DQ policy as PROD and the
pipeline completed successfully through Semantic Model refresh.

## Quality and modeling contract

- Silver uses business-key MERGE logic and a newer `source_updated_at`
  watermark for each new incremental batch.
- Silver can quarantine invalid records; Gold consumes only `VALID` records.
- Missing tables, schema violations, duplicate business keys, required nulls,
  and orphan foreign keys are blocking DQ checks.
- A row-count decrease greater than 30% is recorded as an operational warning
  and the accepted run establishes the next comparison baseline.
- `gld_dim_date` derives coverage from the relevant Silver fact date columns,
  including requested delivery, receipt, dispatch, demand, and inventory dates.
- Semantic Model refresh follows Gold Facts; the report reads through Direct
  Lake from the refreshed model.

## Environment contract

| Environment | Generator | Delivery | Pipeline start | Azure Function |
| --- | --- | --- | --- | --- |
| UAT | Local Python | Manual upload | Manual CLI run | None |
| PROD | Azure Function | Direct OneLake write | Function submission | Enabled |

Keep transformation notebooks aligned between environments, but preserve the
different runtime entry points and workspace bindings.

## Safe operating rules

- Use a new batch ID for every delivered batch.
- Ensure `source_updated_at` is newer than the live Silver watermark.
- Validate manifest counts, key relationships, and representative CSV changes
  before upload.
- Run one bounded pipeline execution at a time.
- Inspect the run-specific status and downstream Semantic Model/report result.
- Keep schedules and event triggers disabled during controlled validation.
- Do not store credentials, Function keys, or connection secrets in the repo.
