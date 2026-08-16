#!/usr/bin/env python3
"""Create one small, linked Bronze batch for incremental-pipeline testing.

The batch has one CSV for each of the 18 existing source entities.  It contains
only one or a few new/updated records per entity, so it is safe to place beside
the 2026/08/11 baseline without re-uploading the full development fixture.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


HEADERS = {
    "crm_customers": "customer_id,customer_name,region,channel,service_tier,latitude,longitude,source_updated_at",
    "erp_locations": "location_id,location_name,region,country_code,daily_capacity_units,storage_capacity_units,source_updated_at",
    "erp_products": "product_id,product_name,category,subcategory,primary_supplier_id,weight_value,weight_uom,case_pack_qty,unit_list_price_thb,shelf_life_days,active_flag,source_updated_at",
    "erp_purchase_orders": "po_id,supplier_id,product_id,destination_location_id,po_date,promised_date,ordered_qty,purchase_uom,unit_cost,currency_code,po_status,source_updated_at",
    "erp_sales_order_lines": "order_id,order_line_no,product_id,fulfillment_location_id,ordered_qty,allocated_qty,order_uom,unit_price_thb,promotion_flag,line_status,source_updated_at",
    "erp_sales_orders": "order_id,customer_id,order_timestamp,requested_delivery_date,sales_channel,order_priority,order_status,source_updated_at",
    "erp_suppliers": "supplier_id,supplier_name,country_code,contract_lead_time_days,payment_terms,active_flag,source_updated_at",
    "finance_logistics_costs": "cost_id,shipment_id,order_id,cost_component,amount,currency_code,posting_date,source_updated_at",
    "planning_demand_forecast": "demand_date,product_id,location_id,forecast_version,forecast_qty,actual_demand_qty,demand_uom,source_updated_at",
    "risk_disruptions": "disruption_id,scope_type,scope_id,event_type,start_date,end_date,severity,delay_multiplier,source_updated_at",
    "tms_carriers": "carrier_id,carrier_name,mode,base_rate_thb_per_kg,fuel_surcharge_pct,source_updated_at",
    "tms_delivery_events": "delivery_event_id,shipment_id,event_sequence,event_type,event_timestamp,event_location,proof_of_delivery_flag,source_updated_at",
    "tms_routes": "route_id,origin_location_id,destination_region,distance_km,standard_transit_days,toll_cost_thb,source_updated_at",
    "tms_shipment_lines": "shipment_id,order_id,order_line_no,product_id,shipped_qty,shipment_uom,source_updated_at",
    "tms_shipments": "shipment_id,order_id,carrier_id,route_id,planned_dispatch_timestamp,actual_dispatch_timestamp,shipment_status,total_weight_value,weight_uom,source_updated_at",
    "wms_activity_events": "wms_event_id,order_id,shipment_id,location_id,event_type,event_timestamp,operator_shift,quantity_processed,exception_code,source_updated_at",
    "wms_inventory_snapshot": "snapshot_date,product_id,location_id,on_hand_qty,reserved_qty,safety_stock_qty,inventory_uom,source_updated_at",
    "wms_purchase_order_receipts": "receipt_id,po_id,product_id,location_id,receipt_timestamp,promised_date,received_qty,received_uom,quality_status,source_updated_at",
}


def rows(batch_date: date, source_updated_at: str, change_set: bool = False) -> dict[str, list[dict[str, str]]]:
    u = source_updated_at
    day0 = batch_date.isoformat()
    day1 = (batch_date + timedelta(days=1)).isoformat()
    day2 = (batch_date + timedelta(days=2)).isoformat()
    batch = {
        "crm_customers": [{"customer_id": "CUS-00001", "customer_name": "Customer 00001", "region": "Metro", "channel": "E-Commerce", "service_tier": "Gold", "latitude": "13.7563", "longitude": "100.5018", "source_updated_at": u}],
        "erp_locations": [{"location_id": "DC-BKK", "location_name": "Bangkok DC", "region": "Metro", "country_code": "TH", "daily_capacity_units": "2800", "storage_capacity_units": "28500", "source_updated_at": u}],
        "erp_products": [
            {"product_id": "SKU-0001", "product_name": "Rice & Grains Product 0001", "category": "Ambient Food", "subcategory": "Rice & Grains", "primary_supplier_id": "SUP-001", "weight_value": "1.2", "weight_uom": "KG", "case_pack_qty": "12", "unit_list_price_thb": "35.00", "shelf_life_days": "365", "active_flag": "Y", "source_updated_at": u},
            {"product_id": "SKU-0002", "product_name": "Rice & Grains Product 0002", "category": "Ambient Food", "subcategory": "Rice & Grains", "primary_supplier_id": "SUP-001", "weight_value": "1.0", "weight_uom": "KG", "case_pack_qty": "12", "unit_list_price_thb": "42.00", "shelf_life_days": "365", "active_flag": "Y", "source_updated_at": u},
        ],
        "erp_purchase_orders": [{"po_id": "PO-00009001", "supplier_id": "SUP-001", "product_id": "SKU-0001", "destination_location_id": "DC-BKK", "po_date": day0, "promised_date": day2, "ordered_qty": "240", "purchase_uom": "EA", "unit_cost": "20.50", "currency_code": "THB", "po_status": "OPEN", "source_updated_at": u}],
        "erp_sales_order_lines": [
            {"order_id": "SO-00002001", "order_line_no": "1", "product_id": "SKU-0001", "fulfillment_location_id": "DC-BKK", "ordered_qty": "12", "allocated_qty": "12", "order_uom": "EA", "unit_price_thb": "35.00", "promotion_flag": "N", "line_status": "FULFILLED", "source_updated_at": u},
            {"order_id": "SO-00002001", "order_line_no": "2", "product_id": "SKU-0002", "fulfillment_location_id": "DC-BKK", "ordered_qty": "6", "allocated_qty": "6", "order_uom": "EA", "unit_price_thb": "42.00", "promotion_flag": "N", "line_status": "FULFILLED", "source_updated_at": u},
        ],
        "erp_sales_orders": [{"order_id": "SO-00002001", "customer_id": "CUS-00001", "order_timestamp": f"{day0}T08:15:00", "requested_delivery_date": day2, "sales_channel": "E-Commerce", "order_priority": "NORMAL", "order_status": "Released", "source_updated_at": u}],
        "erp_suppliers": [{"supplier_id": "SUP-001", "supplier_name": "Supplier 001", "country_code": "TH", "contract_lead_time_days": "7", "payment_terms": "NET30", "active_flag": "Y", "source_updated_at": u}],
        "finance_logistics_costs": [
            {"cost_id": "CST-000009001", "shipment_id": "SHP-00002001", "order_id": "SO-00002001", "cost_component": "FREIGHT", "amount": "425.00", "currency_code": "THB", "posting_date": day2, "source_updated_at": u},
            {"cost_id": "CST-000009002", "shipment_id": "SHP-00002001", "order_id": "SO-00002001", "cost_component": "WAREHOUSE_HANDLING", "amount": "22.40", "currency_code": "THB", "posting_date": day2, "source_updated_at": u},
            {"cost_id": "CST-000009003", "shipment_id": "SHP-00002001", "order_id": "SO-00002001", "cost_component": "FUEL_SURCHARGE", "amount": "42.50", "currency_code": "THB", "posting_date": day2, "source_updated_at": u},
        ],
        "planning_demand_forecast": [{"demand_date": day0, "product_id": "SKU-0001", "location_id": "DC-BKK", "forecast_version": "BASELINE", "forecast_qty": "16", "actual_demand_qty": "12", "demand_uom": "EA", "source_updated_at": u}],
        "risk_disruptions": [{"disruption_id": "DIS-009001", "scope_type": "REGION", "scope_id": "Metro", "event_type": "Capacity Loss", "start_date": day0, "end_date": day2, "severity": "Medium", "delay_multiplier": "1.25", "source_updated_at": u}],
        "tms_carriers": [{"carrier_id": "CAR-01", "carrier_name": "Carrier 01", "mode": "Road", "base_rate_thb_per_kg": "4.20", "fuel_surcharge_pct": "0.10", "source_updated_at": u}],
        "tms_delivery_events": [
            {"delivery_event_id": "DLE-00002001-1", "shipment_id": "SHP-00002001", "event_sequence": "1", "event_type": "PICKED_UP", "event_timestamp": f"{day0}T18:00:00", "event_location": "Metro", "proof_of_delivery_flag": "", "source_updated_at": u},
            {"delivery_event_id": "DLE-00002001-2", "shipment_id": "SHP-00002001", "event_sequence": "2", "event_type": "IN_TRANSIT", "event_timestamp": f"{day1}T03:00:00", "event_location": "Metro", "proof_of_delivery_flag": "", "source_updated_at": u},
            {"delivery_event_id": "DLE-00002001-3", "shipment_id": "SHP-00002001", "event_sequence": "3", "event_type": "DELIVERED", "event_timestamp": f"{day2}T10:00:00", "event_location": "Metro", "proof_of_delivery_flag": "Y", "source_updated_at": u},
        ],
        "tms_routes": [{"route_id": "R-BKK-MET", "origin_location_id": "DC-BKK", "destination_region": "Metro", "distance_km": "45", "standard_transit_days": "1", "toll_cost_thb": "12.00", "source_updated_at": u}],
        "tms_shipment_lines": [
            {"shipment_id": "SHP-00002001", "order_id": "SO-00002001", "order_line_no": "1", "product_id": "SKU-0001", "shipped_qty": "12", "shipment_uom": "EA", "source_updated_at": u},
            {"shipment_id": "SHP-00002001", "order_id": "SO-00002001", "order_line_no": "2", "product_id": "SKU-0002", "shipped_qty": "6", "shipment_uom": "EA", "source_updated_at": u},
        ],
        "tms_shipments": [{"shipment_id": "SHP-00002001", "order_id": "SO-00002001", "carrier_id": "CAR-01", "route_id": "R-BKK-MET", "planned_dispatch_timestamp": f"{day0}T16:00:00", "actual_dispatch_timestamp": f"{day0}T18:00:00", "shipment_status": "DELIVERED", "total_weight_value": "18.0", "weight_uom": "KG", "source_updated_at": u}],
        "wms_activity_events": [
            {"wms_event_id": "WMS-00002001-1", "order_id": "SO-00002001", "shipment_id": "", "location_id": "DC-BKK", "event_type": "PICK_START", "event_timestamp": f"{day0}T09:00:00", "operator_shift": "A", "quantity_processed": "18", "exception_code": "", "source_updated_at": u},
            {"wms_event_id": "WMS-00002001-2", "order_id": "SO-00002001", "shipment_id": "", "location_id": "DC-BKK", "event_type": "PICK_COMPLETE", "event_timestamp": f"{day0}T11:00:00", "operator_shift": "A", "quantity_processed": "18", "exception_code": "", "source_updated_at": u},
            {"wms_event_id": "WMS-00002001-3", "order_id": "SO-00002001", "shipment_id": "", "location_id": "DC-BKK", "event_type": "PACK_COMPLETE", "event_timestamp": f"{day0}T13:00:00", "operator_shift": "B", "quantity_processed": "18", "exception_code": "", "source_updated_at": u},
            {"wms_event_id": "WMS-00002001-4", "order_id": "SO-00002001", "shipment_id": "SHP-00002001", "location_id": "DC-BKK", "event_type": "DISPATCH", "event_timestamp": f"{day0}T18:00:00", "operator_shift": "B", "quantity_processed": "18", "exception_code": "", "source_updated_at": u},
        ],
        "wms_inventory_snapshot": [
            {"snapshot_date": day0, "product_id": "SKU-0001", "location_id": "DC-BKK", "on_hand_qty": "180", "reserved_qty": "12", "safety_stock_qty": "75", "inventory_uom": "EA", "source_updated_at": u},
            {"snapshot_date": day0, "product_id": "SKU-0002", "location_id": "DC-BKK", "on_hand_qty": "145", "reserved_qty": "6", "safety_stock_qty": "70", "inventory_uom": "EA", "source_updated_at": u},
        ],
        "wms_purchase_order_receipts": [{"receipt_id": "REC-00009001", "po_id": "PO-00009001", "product_id": "SKU-0001", "location_id": "DC-BKK", "receipt_timestamp": f"{day0}T14:00:00", "promised_date": day2, "received_qty": "240", "received_uom": "EA", "quality_status": "PASS", "source_updated_at": u}],
    }
    if change_set:
        # Deliberate updates to existing business keys for a second incremental run.
        batch["crm_customers"][0]["service_tier"] = "Platinum"
        batch["erp_locations"][0]["daily_capacity_units"] = "3000"
        batch["erp_products"][0]["unit_list_price_thb"] = "36.50"
        batch["erp_purchase_orders"][0]["ordered_qty"] = "260"
        batch["erp_sales_order_lines"][1]["allocated_qty"] = "5"
        batch["erp_suppliers"][0]["contract_lead_time_days"] = "8"
        batch["finance_logistics_costs"][0]["amount"] = "450.00"
        batch["planning_demand_forecast"][0]["forecast_qty"] = "18"
        batch["risk_disruptions"][0]["severity"] = "High"
        batch["wms_inventory_snapshot"][0]["on_hand_qty"] = "170"
    return batch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="batch date as YYYY/MM/DD; defaults to today in Asia/Bangkok")
    parser.add_argument("--output", type=Path, default=Path("data/bronze_batches"))
    parser.add_argument("--change-set", action="store_true", help="apply deliberate updates for a later incremental batch")
    parser.add_argument(
        "--source-updated-at",
        help="UTC timestamp (YYYY-MM-DDTHH:MM:SS) to use when it must advance an existing Silver watermark",
    )
    args = parser.parse_args()
    batch_day = (datetime.now(ZoneInfo("Asia/Bangkok")).date()
                 if not args.date else date.fromisoformat(args.date.replace("/", "-")))
    args.date = batch_day.strftime("%Y/%m/%d")
    parts = args.date.split("/")
    if len(parts) != 3 or any(not p.isdigit() for p in parts):
        raise SystemExit("--date must be YYYY/MM/DD")

    # Keep ingestion timestamps strictly increasing across same-day batches;
    # Silver watermarks compare this value, not the business batch date.
    source_updated_at = (
        datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
        if not args.source_updated_at
        else datetime.fromisoformat(args.source_updated_at.replace("Z", "+00:00")).replace(tzinfo=None, microsecond=0).isoformat()
    )
    batch = rows(batch_day, source_updated_at, args.change_set)
    counts: dict[str, int] = {}
    for entity, entity_rows in batch.items():
        target = args.output / entity / parts[0] / parts[1] / parts[2] / f"{entity}.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=HEADERS[entity].split(","))
            writer.writeheader()
            writer.writerows(entity_rows)
        counts[entity] = len(entity_rows)

    manifest = {"batch_date": args.date, "source_updated_at": source_updated_at, "entity_count": len(batch), "row_counts": counts}
    manifest_path = args.output / "_batch_manifest_".join(["", args.date.replace("/", "-")])
    manifest_path = manifest_path.with_suffix(".json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
