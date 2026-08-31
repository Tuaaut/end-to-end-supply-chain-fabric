# Raw synthetic data contract

The generator creates linked extracts from fictional source systems. These are
Bronze inputs, not analytics-ready tables. Business keys must be normalized
before joins, timestamps parsed explicitly, duplicates removed, UOM/currency
converted, and business statuses mapped in Silver.

## Raw tables

| Source table | Grain | Main purpose |
| --- | --- | --- |
| `erp_locations` | One distribution center | Capacity and fulfillment node master |
| `erp_suppliers` | One supplier | Supplier terms and contracted lead time |
| `erp_products` | One SKU | Product, category, subcategory, UOM, price, supplier |
| `crm_customers` | One customer | Region, channel, service tier |
| `tms_carriers` | One carrier | Transport mode and freight rates |
| `tms_routes` | One DC-to-region route | Distance, transit standard and toll |
| `risk_disruptions` | One disruption event | Supplier, location, or regional risk |
| `planning_demand_forecast` | Date-SKU-DC-forecast version | Forecast and actual demand |
| `erp_sales_orders` | One sales order | Customer request and promised date |
| `erp_sales_order_lines` | One sales-order line | Ordered, allocated and backordered quantity |
| `wms_inventory_snapshot` | Weekly date-SKU-DC | On-hand, reserved and safety stock |
| `erp_purchase_orders` | One replenishment PO | Supplier order and promised receipt |
| `wms_purchase_order_receipts` | One PO receipt | Actual quantity, date, and quality result |
| `wms_activity_events` | One WMS process event | Pick, pack and dispatch milestones |
| `tms_shipments` | One shipment | Carrier, route, dispatch and weight |
| `tms_shipment_lines` | One shipped order line | Quantity bridge from order to shipment |
| `tms_delivery_events` | One tracking event | Pickup, transit and delivery milestones |
| `finance_logistics_costs` | One shipment cost component | Freight, fuel and warehouse handling cost |

## Raw-data conditions handled by Silver

| Raw issue | Silver transformation | Expected control |
| --- | --- | --- |
| Exact duplicate rows | Deduplicate by natural key, retaining latest extract | Uniqueness test |
| Key whitespace/casing | `trim` and uppercase business IDs | No orphan keys after normalization |
| Three timestamp formats | Parse with explicit ordered format rules | Invalid timestamps quarantined |
| Status synonyms and casing | Map to canonical controlled values | Unmapped-status report |
| `KG`/`G`, `EA`/`Each` | Convert to canonical `KG` and `EA` | Quantity/weight reconciliation |
| THB and USD purchase cost | Join dated FX reference before aggregation | No mixed-currency totals |
| Missing optional attributes | Apply documented rule or retain null | Null-rate monitoring |
| Partial/late receipts and backorders | Preserve as valid business events | Do not treat as data errors |

## Intended lineage

`planning_demand_forecast` -> `erp_sales_orders` / `erp_sales_order_lines` ->
`wms_inventory_snapshot` -> `wms_activity_events` -> `tms_shipments` /
`tms_shipment_lines` -> `tms_delivery_events` -> `finance_logistics_costs`.

The fixed random seed makes each profile reproducible. The `dev` profile is for
fast local iteration. The `full` profile targets 24 months, 300 SKUs, 1,000
customers and approximately 150,000 order lines.

## Bronze batch layout

Use one entity folder per source and partition each upload by ingestion date:

```text
Files/bronze/<entity>/YYYY/MM/DD/<entity>.csv
```

Keep the CSV filename stable, retain prior partitions, and upload new batches as
new date folders. The incremental Silver notebooks read entity folders
recursively and use file watermarks plus business-key MERGE logic.

## Product mapping reference

`erp_products` maps to `slv_product` at one current conformed row per normalized
`product_id`. Normalize keys with trim and uppercase, preserve category and
subcategory, convert weight to kilograms, map `active_flag` to `is_active`, and
quarantine invalid keys, hierarchy pairs, suppliers, timestamps, or non-positive
quantities.
