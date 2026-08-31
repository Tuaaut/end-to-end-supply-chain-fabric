# Portfolio Findings & Business Case

Last updated: 2026-08-19 (Bangkok)

## The business case

Supply chain decisions — which carrier to use, how much safety stock to
hold, where cost is leaking — are usually made from spreadsheets that are
already a week stale by the time anyone reads them. This project builds a
**control tower**: one place where planning, fulfillment, transportation,
inventory, and logistics cost data land, get validated, and show up in a
report the same day, with a governed data-quality gate sitting in front of
every number so "the dashboard says X" actually means something.

It runs as two parallel environments — a manual UAT for testing changes
safely, and an event-driven PROD path that ingests real batches
automatically — both built on Microsoft Fabric, OneLake, and Power BI.

## Key findings

Building and hardening this pipeline surfaced five real data-quality bugs.
Each one silently distorted what leadership would have seen on the
dashboard — none were visible from the report itself, only by tracing the
data back through the pipeline.

| # | What was silently wrong | Business impact | Status |
| --- | --- | --- | --- |
| 1 | A validation rule required "proof of delivery" on every shipment event, but that field is only ever filled in once a shipment is *actually delivered* | 100% of "picked up" and "in transit" events were invisible on the Transportation & Shipping report — leadership could only ever see completed deliveries, never shipments in motion | Fixed, and the ~3,150-event historical backlog was recovered — see the [current Delivery Event Mix](#screenshot-transportation), now split evenly across all three event types |
| 2 | The same class of bug in 4 more tables (demand forecasts, inventory snapshots, and two tables that silently dropped bad rows with no audit trail) | Understated forecast coverage, false inventory-quality flags, and quality issues with zero visibility for two entire data sources | Fixed |
| 3 | PROD's data-quality gate — the automated check that's supposed to block bad data from ever reaching Gold/report — had been silently validating against the *test* environment's data instead of PROD's own data | PROD's "quality gate" had not been genuinely protecting PROD data since it was built. Per-row quarantine still worked, but the cross-table integrity checks (duplicate keys, orphaned references) were not | Found and fixed; PROD's gate now genuinely re-validates PROD's own data every run |
| 4 | A report control ("What-If" scenario slider) silently pointed at the wrong data column after being published | The Scenarios & Recommendations page's cost/OTIF sliders would have shown wrong or blank numbers with no error | Fixed |
| 5 | The data-quality dashboard's own "failed checks" counter read the wrong column internally | The DQ Score tile understated data quality — it counted every check's *severity category* as a failure, not its actual pass/fail result | Fixed |

**Why this matters for the business case:** every one of these bugs would
have shipped invisibly into a leadership dashboard. None threw an error;
none looked wrong at a glance. This is the argument for the governed
pipeline itself — a spreadsheet has no equivalent of a data-quality gate
that can be checked, tested, and audited.

## The dashboard today (live snapshot, 2026-08-19)

<a id="screenshot-transportation"></a>

| Report page | Headline numbers |
| --- | --- |
| **Executive Overview** | ฿1.69M total sales · 95.7% fill rate · 77.1% forecast accuracy |
| **Transportation & Shipping** | 13.7% OTIF · 29.2% on-time dispatch · delivery events now split ~evenly across Picked Up / In Transit / Delivered (was 100% Delivered-only before fix #1) |
| **Network & Cost-to-Serve** | ฿780.56K total logistics cost · 88% of cost is freight, not warehouse handling |
| **Scenarios & Recommendations** | A 10% cost-savings target + 5% OTIF-improvement target models to **฿78.06K in savings** and OTIF rising from 13.7% → 18.7% — see [Recommendations](#recommendations) |
| **Data Quality Dashboard** | Historical DQ Score trend (48.5 as of the last batch) reflects the *pre-fix* scoring bug (#5) — the score itself was undercounting; the underlying gate logic was never wrong. Next batch will report a corrected score. |

A full 8-page PDF export of the live report (all pages, current filter/slicer
state) is at [docs/screenshots/rpt_supply_chain_UAT-2026-08-19.pdf](docs/screenshots/rpt_supply_chain_UAT-2026-08-19.pdf),
with per-page PNGs alongside it in [docs/screenshots/](docs/screenshots/)
(three are embedded in the [README](README.md#report-snapshot)).

## Limitations

- **No SKU-level cost-to-serve.** Logistics cost is captured at
  shipment/order grain; shipment-line quantity was never promoted to Gold,
  so there's no reliable basis to allocate cost down to a product. Cost
  reporting is scoped to customer/carrier/route/region.
- **No physical "scenario" dimension.** There's no scenario source data
  anywhere in the pipeline, so Scenarios & Recommendations is built as
  live Power BI What-If sliders over real measures, not a saved list of
  named scenarios.
- **Two of five historical DQ bugs' backlogs were not reprocessed on
  PROD** — PROD's data was reset to a single clean batch generated after
  the code fixes, so it never accumulated the backlog UAT had. UAT's own
  backlog (bug #1) was reprocessed and recovered.
- **Mechanical test batches, not organic ones.** The PROD validation
  batches used near-fixed relative timestamps rather than the randomized
  generator, so metrics like OTIF % can look artificially uniform in
  those specific batches — a data-generation artifact, not a pipeline bug.

## Recommendations

<a id="recommendations"></a>

Grounded directly in the What-If model on the Scenarios & Recommendations
page — not hypothetical numbers:

1. **Consolidate carrier volume.** Negotiate volume-based freight rates
   with the highest-cost carriers before adding capacity elsewhere —
   freight is 88% of total logistics cost, the single largest lever
   available.
2. **Close the OTIF gap carrier-by-carrier.** Prioritize dispatch-time
   fixes for carriers performing below the fleet average OTIF before
   committing to a network-wide service-level target.
3. **Model cost and service together, not separately.** The scenario
   model shows a 10% cost-savings target and a 5% OTIF-improvement target
   can be pursued simultaneously (฿78.06K saved, OTIF +5 points in the
   model) — but treat these as directional planning inputs, not committed
   figures. Validate against live carrier contract terms before finalizing
   a routing plan.

## Appendix: how the findings were verified

Every finding above was confirmed against live data, not inferred from
code review alone — the standard this project holds itself to throughout
(see `CURRENT_STATUS.md` for the full technical log).

- **Finding #1** (`proof_of_delivery_flag`): confirmed via live DAX query
  that `gld_fact_delivery_event` held only `DELIVERED` events before the
  fix (1,563 rows, zero of anything else); after the code fix *and* a
  one-time backfill of the historical backlog, the same query returned
  `DELIVERED` 1,563 (unchanged — no over-correction), `PICKED_UP` 1,579,
  `IN_TRANSIT` 1,573.
- **Finding #3** (PROD Lakehouse misattachment): confirmed by checking
  **UAT's own** pipeline run log and finding a fresh notebook run logged
  there at the exact timestamp of a PROD pipeline run — proof the PROD
  notebook's Spark session had been silently reading and writing against
  UAT's Lakehouse. Fixed, then reverified with a second live PROD batch
  that completed fully with the gate genuinely re-validating PROD data.
- **Finding #4** (What-If `sourceColumn`): the import itself returned no
  error — this was only caught by diffing the post-import re-export
  against what was pushed, then confirmed broken/fixed via a live DAX
  query against the model, not just a visual check of the report.
- **Finding #5** (DQ score column bug): confirmed the fix landed on both
  UAT and PROD via re-export verification; not yet validated against a
  live batch with real `FAIL` rows (the next batch's DQ Score should be
  spot-checked against `ops_data_quality_summary`'s actual `status`
  counts).

Full root-cause narratives, verification steps, and every other fix made
during this project are documented in `CURRENT_STATUS.md`.
