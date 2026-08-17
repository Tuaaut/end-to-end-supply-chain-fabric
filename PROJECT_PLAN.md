# End-to-End Supply Chain Control Tower & Optimization on Microsoft Fabric

> This document records the original scope, structure, and phased plan. For
> where the project actually stands today, see
> [CURRENT_STATUS.md](CURRENT_STATUS.md).

## 1. Project purpose

Build a portfolio project that demonstrates business analysis, supply chain analytics, KPI design, scenario modeling, data engineering, semantic modeling, and executive reporting in one coherent solution.

The project represents a fictional multi-region distributor. It is intentionally industry-neutral so that the portfolio remains relevant to retail, FMCG, manufacturing, healthcare, distribution, and logistics consulting roles.

### Portfolio positioning

> Business and Supply Chain Analyst who can analyze operations, define KPIs, model scenarios, and build the analytical solution end-to-end.

### Main business decision

How should the company balance customer service, inventory, warehouse capacity, transportation performance, supply-chain risk, and total logistics cost?

## 2. Agreed project structure

- One GitHub repository
- One integrated fictional company dataset
- Three analytical modules
- One Power BI report with an executive control tower
- Microsoft Fabric and Power BI as the primary technology stack
- No direct row-level merge between unrelated public-company datasets
- Public datasets are used only as references, calibration inputs, or valid external enrichment

## 3. Business scope

### Module 1: Planning - Demand, inventory, and suppliers

Business questions:

- What demand should the company expect by product, location, and period?
- Which SKUs are at risk of stockout or excess inventory?
- How much safety stock is required?
- Which suppliers create the greatest lead-time risk?

### Module 2: Fulfillment - Warehouse, orders, shipping, and transportation

Business questions:

- Can warehouse capacity support normal and peak demand?
- Where does order-cycle delay occur?
- Are shipments dispatched and delivered on time?
- Which carrier, route, region, or warehouse performs poorly?
- How do warehouse and transportation performance affect OTIF?

### Module 3: Optimization - Network, cost-to-serve, and resilience

Business questions:

- What is the logistics cost by customer, SKU, order, region, and channel?
- Which customers or SKUs have a high cost-to-serve?
- How should orders be allocated across warehouses and carriers?
- What is the service and cost impact of additional capacity or disruption?

## 4. Data-source strategy

The final analytical dataset will describe one fictional company. It will be generated from a common data model so that all keys, events, quantities, dates, and costs remain internally consistent.

| Module | Generated operational data | Public or external reference data |
| --- | --- | --- |
| Planning | Demand, forecast, inventory, purchase orders, supplier receipts | M5 patterns for seasonality, promotions, prices, and demand variability |
| Fulfillment | Sales orders, WMS activities, shipments, routes, delivery events | DataCo structure for order, shipping, and delivery concepts |
| Optimization | Facility capacity, carriers, routes, disruptions, logistics costs | World Bank LPI, distance, country-risk, and other valid external data |

### How public datasets will be used

- M5 will inform realistic demand distributions and seasonal patterns. Its rows will not be merged into the fictional company.
- DataCo will inform order, shipping, and delivery structures. Its company records will not be mixed with M5 records.
- World Bank LPI may be used as real external enrichment where a valid country-and-year key exists.
- Operational data will be generated using Python in a Fabric Notebook with a fixed random seed.

### Reference sources

- [M5 competition data and methods](https://github.com/Mcompetitions/M5-methods)
- [DataCo Smart Supply Chain dataset](https://data.mendeley.com/datasets/8gx2fvg2k6/5)
- [World Bank Logistics Performance Index](https://datacatalog.worldbank.org/search/dataset/0038649/logistics-performance-index)

## 5. Synthetic-data scale

- 24 months of activity
- 300 SKUs across 5 product categories
- 3 distribution centers
- 20 suppliers
- 6 carriers
- 1,000 customers across 8 regions
- Approximately 150,000 sales-order lines

The active event-batch profile is approximately 1,600 orders and 4,800
order lines per batch. The portfolio-scale generator remains available for
the broader analytical dataset when capacity and reporting scope require it.

## 6. Data-generation logic

The generator must create dependent events in business order rather than generating unrelated tables independently.

```text
Configuration and scenario parameters
                |
                v
Products, suppliers, locations, customers, carriers, routes
                |
                v
Demand and forecasts
                |
                v
Sales orders and inventory consumption
                |
                v
Replenishment and purchase orders
                |
                v
WMS receiving, putaway, picking, packing, and dispatch
                |
                v
Shipment, route, carrier, and delivery events
                |
                v
Warehouse, inventory, and transportation costs
```

### Required simulated conditions

- Seasonal demand
- Promotion-driven demand spikes
- Supplier delay and lead-time variability
- Stockout and excess inventory
- Warehouse congestion and capacity constraints
- Picking delays and accuracy issues
- Late carrier dispatch and delivery
- Freight-cost variation
- Regional or supplier disruption

## 7. Canonical data model

### Dimensions

- `dim_date`
- `dim_product`
- `dim_customer`
- `dim_supplier`
- `dim_location`
- `dim_carrier`
- `dim_route`
- `dim_scenario`

### Facts

- `fact_demand_forecast`
- `fact_sales_order_line`
- `fact_inventory_snapshot`
- `fact_purchase_order_receipt`
- `fact_wms_activity`
- `fact_shipment`
- `fact_delivery_event`
- `fact_logistics_cost`
- `fact_disruption_event`

### Required lineage

Every fulfilled order should be traceable through:

```text
Demand -> Sales order -> Inventory -> Pick/pack -> Shipment -> Delivery -> Cost
```

## 8. KPI framework

| Module | Primary KPIs | Diagnostic drivers | Guardrails |
| --- | --- | --- | --- |
| Planning | Forecast WAPE, Inventory Days, Stockout Rate | Forecast Bias, Supplier Lead-Time Variability, Safety-Stock Coverage | Excess Inventory, Obsolescence |
| Fulfillment | OTIF, Order Cycle Time, Warehouse Throughput | Pick Rate, On-Time Dispatch, Dock-to-Stock Time, Load Factor | Picking Accuracy, Damage Rate, Overtime |
| Optimization | Cost-to-Serve, Logistics Cost per Order, Capacity Utilization | Distance, Freight Rate, Carrier Performance, Node Throughput | Service Coverage, Supplier and Carrier Concentration |

Targets will not be invented before a baseline exists. Initial targets will be derived from the generated baseline distribution and the expected impact of controlled scenarios.

## 9. Current technology architecture

```text
UAT: Local generator -> manual Bronze upload -> UAT pipeline
                                                    |
PROD: Azure Function -> PROD OneLake Bronze -> PROD pipeline
                                                    v
                              Silver -> Gold Dimensions -> Gold Facts
                                                    |
                                  Direct Lake Semantic Model
                                                    |
                                             Power BI report
```

UAT and PROD use separate workspaces, Lakehouses, pipelines, and runtime entry
points. They share the data contract and transformation design. UAT is the
controlled manual validation path; PROD is the event-driven Function path.
Both paths have passed the current end-to-end validation, and schedules and
event triggers remain disabled.

### Component responsibilities

| Layer | Technology | Responsibility |
| --- | --- | --- |
| Ingestion and orchestration | UAT manual upload / PROD Azure Function and Fabric Pipeline | Deliver Bronze inputs and coordinate notebooks and transformations |
| Optional low-code preparation | Dataflow Gen2 | Demonstrate one simple cleansing or mapping workflow |
| Storage | OneLake Lakehouse | Store Bronze, Silver, and Gold Delta tables |
| Advanced analysis | Fabric Notebook | Transform data, forecast demand, run scenarios, and perform optimization |
| Business transformation | SQL | Validate data and create Gold analytical tables |
| Business model | Power BI semantic model and DAX | Define relationships, measures, and reusable KPI logic |
| Visualization | Power BI | Deliver decision-oriented dashboards and recommendations |
| Version control | GitHub | Store code, documentation, diagrams, and approved report evidence |

The current event-driven integration uses Azure Functions only for the PROD
entry point. Event Hubs, Azure Data Factory, Synapse, and Databricks are not part
of the current runtime path.

## 10. Power BI report design

### Page 1: Executive Control Tower

- OTIF
- Forecast accuracy
- Inventory days
- Cost-to-serve
- Current capacity and risk alerts

### Page 2: Demand and Inventory

- Forecast versus actual demand
- Stockout and excess-inventory trends
- SKU segmentation
- Supplier and replenishment performance

### Page 3: Warehouse and Fulfillment

- Receiving, putaway, picking, packing, and dispatch throughput
- Capacity utilization
- Order-cycle decomposition
- Bottlenecks by warehouse, shift, activity, and SKU profile

### Page 4: Transportation and Shipping

- On-time dispatch and on-time delivery
- Carrier OTIF and delivery delay
- Transit time and load factor
- Freight cost by carrier, route, region, and shipment

### Page 5: Network and Cost-to-Serve

- Logistics cost by customer, SKU, region, and channel
- Facility and route utilization
- High-cost and low-margin segments

### Page 6: Scenarios and Recommendations

- Baseline versus proposed scenario
- Service, inventory, capacity, and cost impact
- Recommended action and implementation considerations

## 11. Analytical use cases

- Demand forecasting
- ABC/XYZ SKU segmentation
- Safety-stock and reorder-point simulation
- Warehouse capacity and bottleneck analysis
- Order-delay root-cause analysis
- Carrier and route performance analysis
- Customer and SKU cost-to-serve
- Distribution-network scenario comparison

The MVP should contain only one deep optimization showcase. The preferred starting choices are inventory policy optimization or warehouse-to-customer allocation.

## 12. Required deliverables

- Reproducible synthetic-data generator
- KPI dictionary with definitions and formulas
- Fabric ingestion/orchestration pipeline
- Bronze, Silver, and Gold Lakehouse layers
- Forecasting and scenario notebooks
- Gold star schema
- Power BI semantic model and report
- Architecture and data-lineage diagrams
- Executive findings and recommendations
- GitHub README with screenshots, methodology, assumptions, and limitations

## 13. Quality and validation requirements

- No orphan dimension keys
- Quantities reconcile from order through shipment and delivery
- Inventory never changes without a related business event
- Shipment and cost records reconcile to fulfilled orders
- Gold KPI calculations reconcile with Power BI measures
- Synthetic and external data are clearly labeled
- No confidential employer or customer data is used
- At least three decision-ready recommendations are supported by the analysis

## 14. Implementation phases

### Phase 0: Confirm the data contract

- Freeze dimensions, facts, grains, keys, and business rules
- Define KPI formulas and scenario parameters

### Phase 1: Build the synthetic-data generator

- Generate conformed master data
- Generate dependent operational events
- Validate keys, quantities, dates, and costs locally

### Phase 2: Implement the Fabric medallion pipeline

- Load raw files into Bronze
- Clean and conform records in Silver
- Build Gold facts, dimensions, and KPI tables

### Phase 3: Build the analytical modules

- Planning and inventory analysis
- Warehouse, fulfillment, transportation, and shipping analysis
- Network, cost-to-serve, and disruption scenarios

### Phase 4: Build the semantic model and Power BI report

- Create relationships and measures
- Build the executive page and module pages
- Validate DAX against Gold-layer results

### Phase 5: Document and publish the portfolio

- Write the business case, findings, limitations, and recommendations
- Add architecture, lineage, and report screenshots
- Perform a focused QA pass before publishing

## 15. Estimated effort

- MVP: approximately 28-38 hours
- Fully polished portfolio version: approximately 40-50 hours

Implementation should proceed one phase at a time. Before any billable Fabric capacity is resumed or a capacity-consuming notebook/pipeline is run, confirm cost impact and current capacity choice.

## 16. Starting point for the next session

Phases 0-4 are complete: both the UAT and PROD paths have passed end-to-end
validation through Silver, Gold, the Semantic Model, and the Power BI report.
See [CURRENT_STATUS.md](CURRENT_STATUS.md) for the validated checkpoints and
[OPERATIONS.md](OPERATIONS.md) for how to run a new batch. Continue from
Phase 5 (document and publish) unless a specific regression or new phase of
work is agreed first. As before, do not resume billable Fabric capacity or run
a capacity-consuming notebook/pipeline without confirming cost impact first.
