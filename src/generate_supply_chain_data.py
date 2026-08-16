#!/usr/bin/env python3
"""Generate linked, intentionally imperfect supply-chain source data.

Only the Python standard library is required.  The raw files mimic extracts
from ERP, WMS, TMS and planning systems; they are not Silver-ready by design.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


PROFILES = {
    "dev": {"days": 90, "products": 50, "customers": 200, "suppliers": 8,
            "carriers": 4, "order_lines": 5_000},
    "full": {"days": 730, "products": 300, "customers": 1_000, "suppliers": 20,
             "carriers": 6, "order_lines": 150_000},
}

CATEGORIES = {
    "Ambient Food": (0.3, 1.8, 4.0, 28.0),
    "Beverage": (0.5, 6.0, 3.0, 18.0),
    "Personal Care": (0.1, 1.2, 6.0, 45.0),
    "Household": (0.2, 4.0, 8.0, 80.0),
    "Healthcare": (0.05, 0.8, 12.0, 120.0),
}
SUBCATEGORIES = {
    "Ambient Food": ["Rice & Grains", "Canned Food", "Snacks", "Cooking Essentials"],
    "Beverage": ["Water", "Juice", "Carbonated Drinks", "Functional Drinks"],
    "Personal Care": ["Hair Care", "Skin Care", "Oral Care", "Body Care"],
    "Household": ["Laundry", "Home Cleaning", "Kitchen Care", "Paper Products"],
    "Healthcare": ["First Aid", "Supplements", "Medical Supplies", "Wellness"],
}
REGIONS = ["North", "Northeast", "Central", "East", "West", "South", "Metro", "Coastal"]
REGION_CODES = {"North": "NTH", "Northeast": "NEA", "Central": "CEN", "East": "EST",
                "West": "WST", "South": "STH", "Metro": "MET", "Coastal": "COA"}
CHANNELS = ["Retail", "Wholesale", "E-Commerce"]


def iso(dt: datetime | date) -> str:
    return dt.isoformat(timespec="seconds") if isinstance(dt, datetime) else dt.isoformat()


def dirty_text(value: str, rng: random.Random, rate: float = 0.025) -> str:
    """Add common source-system casing/whitespace problems."""
    roll = rng.random()
    if roll < rate / 3:
        return f" {value} "
    if roll < rate * 2 / 3:
        return value.lower()
    if roll < rate:
        return value.upper()
    return value


def dirty_date(value: datetime, rng: random.Random) -> str:
    """Return one of three formats to force explicit parsing in Silver."""
    roll = rng.random()
    if roll < 0.025:
        return value.strftime("%d/%m/%Y %H:%M")
    if roll < 0.05:
        return value.strftime("%m/%d/%Y %I:%M %p")
    return value.isoformat(timespec="seconds")


def write_csv(path: Path, rows: list[dict], duplicate_rate: float, rng: random.Random) -> int:
    if not rows:
        return 0
    output = list(rows)
    duplicate_count = round(len(rows) * duplicate_rate)
    if duplicate_count:
        output.extend(dict(row) for row in rng.sample(rows, min(duplicate_count, len(rows))))
    rng.shuffle(output)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(output)
    return len(output)


def generate(profile: str, seed: int, output_dir: Path) -> dict:
    cfg = PROFILES[profile]
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    end = start + timedelta(days=cfg["days"] - 1)
    extracted_at = datetime.combine(end + timedelta(days=2), datetime.min.time()).replace(hour=6)

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    tables: dict[str, list[dict]] = {}

    locations = [
        {"location_id": "DC-BKK", "location_name": "Bangkok DC", "region": "Metro",
         "country_code": "TH", "daily_capacity_units": 2600, "storage_capacity_units": 28000},
        {"location_id": "DC-CNX", "location_name": "Chiang Mai DC", "region": "North",
         "country_code": "TH", "daily_capacity_units": 1400, "storage_capacity_units": 16000},
        {"location_id": "DC-HDY", "location_name": "Hat Yai DC", "region": "South",
         "country_code": "TH", "daily_capacity_units": 1300, "storage_capacity_units": 15000},
    ]
    for row in locations:
        row["source_updated_at"] = iso(extracted_at)
    tables["erp_locations"] = locations

    suppliers = []
    for i in range(1, cfg["suppliers"] + 1):
        suppliers.append({
            "supplier_id": f"SUP-{i:03d}", "supplier_name": f"Supplier {i:03d}",
            "country_code": rng.choice(["TH", "TH", "VN", "MY", "CN"]),
            "contract_lead_time_days": rng.choice([5, 7, 10, 14, 21, 30]),
            "payment_terms": rng.choice(["NET30", "Net 30", "NET45", "NET60"]),
            "active_flag": rng.choice(["Y", "Y", "Y", "1"]), "source_updated_at": iso(extracted_at),
        })
    tables["erp_suppliers"] = suppliers

    products = []
    product_truth = {}
    product_hierarchy = [(category, subcategory) for category, values in SUBCATEGORIES.items()
                         for subcategory in values]
    for i in range(1, cfg["products"] + 1):
        # Guarantee complete hierarchy coverage before adding random assortment depth.
        if i <= len(product_hierarchy):
            category, subcategory = product_hierarchy[i - 1]
        else:
            category = rng.choice(list(CATEGORIES))
            subcategory = rng.choice(SUBCATEGORIES[category])
        min_w, max_w, min_p, max_p = CATEGORIES[category]
        supplier = rng.choice(suppliers)["supplier_id"]
        weight_kg = round(rng.uniform(min_w, max_w), 3)
        case_pack = rng.choice([6, 12, 24])
        product_id = f"SKU-{i:04d}"
        product_truth[product_id] = {"supplier_id": supplier, "weight_kg": weight_kg,
                                     "case_pack": case_pack, "price": round(rng.uniform(min_p, max_p), 2)}
        # Some rows arrive in grams, requiring UOM normalization.
        use_grams = rng.random() < 0.12
        products.append({
            "product_id": product_id, "product_name": f"{subcategory} Product {i:04d}",
            "category": dirty_text(category, rng), "subcategory": dirty_text(subcategory, rng),
            "primary_supplier_id": dirty_text(supplier, rng),
            "weight_value": round(weight_kg * 1000, 1) if use_grams else weight_kg,
            "weight_uom": "G" if use_grams else "KG", "case_pack_qty": case_pack,
            "unit_list_price_thb": product_truth[product_id]["price"],
            "shelf_life_days": rng.choice([180, 365, 540, ""]), "active_flag": "Y",
            "source_updated_at": iso(extracted_at),
        })
    tables["erp_products"] = products

    customers = []
    for i in range(1, cfg["customers"] + 1):
        customers.append({
            "customer_id": f"CUS-{i:05d}", "customer_name": f"Customer {i:05d}",
            "region": dirty_text(rng.choice(REGIONS), rng), "channel": dirty_text(rng.choice(CHANNELS), rng),
            "service_tier": rng.choices(["Gold", "Silver", "Standard"], [0.15, 0.35, 0.5])[0],
            "latitude": round(rng.uniform(6.0, 20.0), 5), "longitude": round(rng.uniform(98.0, 105.5), 5),
            "source_updated_at": iso(extracted_at),
        })
    tables["crm_customers"] = customers

    carriers = []
    for i in range(1, cfg["carriers"] + 1):
        carriers.append({
            "carrier_id": f"CAR-{i:02d}", "carrier_name": f"Carrier {i:02d}",
            "mode": rng.choice(["Road", "ROAD", "Parcel"]), "base_rate_thb_per_kg": round(rng.uniform(2.8, 5.5), 2),
            "fuel_surcharge_pct": round(rng.uniform(0.05, 0.14), 3), "source_updated_at": iso(extracted_at),
        })
    tables["tms_carriers"] = carriers

    routes = []
    for loc in locations:
        for region in REGIONS:
            distance = rng.randint(45, 1250)
            routes.append({
                "route_id": f"R-{loc['location_id'][-3:]}-{REGION_CODES[region]}",
                "origin_location_id": loc["location_id"], "destination_region": dirty_text(region, rng),
                "distance_km": distance, "standard_transit_days": max(1, math.ceil(distance / 450)),
                "toll_cost_thb": round(distance * rng.uniform(0.12, 0.35), 2), "source_updated_at": iso(extracted_at),
            })
    tables["tms_routes"] = routes

    disruptions = []
    for i in range(max(3, cfg["days"] // 45)):
        event_start = start + timedelta(days=rng.randint(5, cfg["days"] - 5))
        duration = rng.randint(2, 8)
        scope_type = rng.choice(["SUPPLIER", "LOCATION", "REGION"])
        scope_id = (rng.choice(suppliers)["supplier_id"] if scope_type == "SUPPLIER" else
                    rng.choice(locations)["location_id"] if scope_type == "LOCATION" else rng.choice(REGIONS))
        disruptions.append({
            "disruption_id": f"DIS-{i + 1:04d}", "scope_type": scope_type, "scope_id": scope_id,
            "event_type": rng.choice(["Port Delay", "Flood", "Capacity Loss", "Supplier Shortage"]),
            "start_date": iso(event_start), "end_date": iso(event_start + timedelta(days=duration)),
            "severity": rng.choice(["Medium", "High"]), "delay_multiplier": rng.choice([1.25, 1.5, 2.0]),
            "source_updated_at": iso(extracted_at),
        })
    tables["risk_disruptions"] = disruptions

    # Operational simulation state.
    inventory = {(p["product_id"], loc["location_id"]): rng.randint(80, 300)
                 for p in products for loc in locations}
    reorder = {(p["product_id"], loc["location_id"]): rng.randint(45, 100)
               for p in products for loc in locations}
    customer_by_region = defaultdict(list)
    for customer in customers:
        customer_by_region[customer["region"].strip().title()].append(customer)

    orders, lines, demand, inventory_snapshots = [], [], [], []
    purchase_orders, receipts, wms_events = [], [], []
    shipments, shipment_lines, delivery_events, costs = [], [], [], []
    actual_demand = defaultdict(int)
    line_counter = order_counter = po_counter = shipment_counter = 0
    pending_receipts: dict[date, list[tuple[str, str, int, str, date]]] = defaultdict(list)
    lines_per_day = cfg["order_lines"] / cfg["days"]

    for day_idx in range(cfg["days"]):
        current = start + timedelta(days=day_idx)
        # Apply receipts first, preserving late and partial receipt behavior.
        for po_id, product_id, qty, location_id, promised in pending_receipts.pop(current, []):
            received = max(1, round(qty * rng.uniform(0.82, 1.0)))
            inventory[(product_id, location_id)] += received
            receipts.append({
                "receipt_id": f"REC-{len(receipts) + 1:07d}", "po_id": dirty_text(po_id, rng),
                "product_id": dirty_text(product_id, rng), "location_id": location_id,
                "receipt_timestamp": dirty_date(datetime.combine(current, datetime.min.time()).replace(hour=rng.randint(6, 18)), rng),
                "promised_date": iso(promised), "received_qty": received,
                "received_uom": rng.choice(["EA", "EA", "Each"]), "quality_status": rng.choice(["PASS", "Pass", "HOLD"]),
                "source_updated_at": iso(extracted_at),
            })

        target_today = max(1, round(lines_per_day * rng.uniform(0.72, 1.28)))
        created_today = 0
        while line_counter < cfg["order_lines"] and created_today < target_today:
            order_counter += 1
            region = rng.choice(REGIONS)
            customer = rng.choice(customer_by_region[region]) if customer_by_region[region] else rng.choice(customers)
            location = rng.choice(locations)
            order_id = f"SO-{order_counter:08d}"
            order_ts = datetime.combine(current, datetime.min.time()).replace(hour=rng.randint(7, 18), minute=rng.randint(0, 59))
            requested = current + timedelta(days=rng.choice([1, 2, 3, 5]))
            orders.append({
                "order_id": order_id, "customer_id": dirty_text(customer["customer_id"], rng),
                "order_timestamp": dirty_date(order_ts, rng), "requested_delivery_date": iso(requested),
                "sales_channel": dirty_text(customer["channel"].strip().title(), rng),
                "order_priority": rng.choice(["NORMAL", "Normal", "EXPEDITE"]),
                "order_status": "Released", "source_updated_at": iso(extracted_at),
            })
            order_lines_this = min(rng.randint(1, 5), cfg["order_lines"] - line_counter,
                                   target_today - created_today)
            order_fulfilled = []
            for seq in range(1, order_lines_this + 1):
                line_counter += 1
                created_today += 1
                product = rng.choice(products)
                product_id = product["product_id"]
                season = 1 + 0.22 * math.sin(2 * math.pi * day_idx / 365)
                promotion = rng.random() < 0.06
                ordered_qty = max(1, round(rng.randint(1, 18) * season * (rng.uniform(1.5, 2.8) if promotion else 1)))
                key = (product_id, location["location_id"])
                available = inventory[key]
                allocated = min(ordered_qty, available)
                inventory[key] -= allocated
                unit_price = product_truth[product_id]["price"] * (rng.uniform(0.82, 0.95) if promotion else 1)
                status = "FULFILLED" if allocated == ordered_qty else "PARTIAL" if allocated else "BACKORDER"
                lines.append({
                    "order_id": dirty_text(order_id, rng), "order_line_no": seq,
                    "product_id": dirty_text(product_id, rng), "fulfillment_location_id": dirty_text(location["location_id"], rng),
                    "ordered_qty": ordered_qty, "allocated_qty": allocated, "order_uom": rng.choice(["EA", "EA", "Each"]),
                    "unit_price_thb": round(unit_price, 2), "promotion_flag": "Y" if promotion else "N",
                    "line_status": dirty_text(status, rng), "source_updated_at": iso(extracted_at),
                })
                actual_demand[(current, product_id, location["location_id"])] += ordered_qty
                if allocated:
                    order_fulfilled.append((line_counter, seq, product_id, allocated))

                if inventory[key] < reorder[key] and not any(x[1] == product_id and x[3] == location["location_id"] for xs in pending_receipts.values() for x in xs):
                    po_counter += 1
                    supplier_id = product_truth[product_id]["supplier_id"]
                    supplier = next(s for s in suppliers if s["supplier_id"] == supplier_id)
                    po_qty = rng.randint(4, 8) * product_truth[product_id]["case_pack"] * 5
                    lead = supplier["contract_lead_time_days"] + rng.randint(-2, 8)
                    promised = current + timedelta(days=supplier["contract_lead_time_days"])
                    actual_receipt = min(end, current + timedelta(days=max(1, lead)))
                    po_id = f"PO-{po_counter:07d}"
                    purchase_orders.append({
                        "po_id": po_id, "supplier_id": dirty_text(supplier_id, rng), "product_id": product_id,
                        "destination_location_id": location["location_id"], "po_date": iso(current),
                        "promised_date": iso(promised), "ordered_qty": po_qty, "purchase_uom": "EA",
                        "unit_cost": round(product_truth[product_id]["price"] * rng.uniform(0.52, 0.72), 2),
                        "currency_code": rng.choice(["THB", "THB", "USD"]), "po_status": "OPEN",
                        "source_updated_at": iso(extracted_at),
                    })
                    pending_receipts[actual_receipt].append((po_id, product_id, po_qty, location["location_id"], promised))

            if order_fulfilled:
                shipment_counter += 1
                shipment_id = f"SHP-{shipment_counter:08d}"
                route = next(r for r in routes if r["origin_location_id"] == location["location_id"]
                             and r["destination_region"].strip().title() == region)
                carrier = rng.choice(carriers)
                congestion = day_idx % 30 in range(24, 30)
                dispatch_delay = rng.choice([0, 0, 0, 1, 2]) + (1 if congestion else 0)
                dispatch_dt = order_ts + timedelta(days=dispatch_delay, hours=rng.randint(4, 20))
                transit_days = route["standard_transit_days"] + rng.choice([0, 0, 1, 2])
                delivery_dt = dispatch_dt + timedelta(days=transit_days, hours=rng.randint(1, 12))
                total_weight = sum(qty * product_truth[pid]["weight_kg"] for _, _, pid, qty in order_fulfilled)
                shipments.append({
                    "shipment_id": shipment_id, "order_id": dirty_text(order_id, rng), "carrier_id": carrier["carrier_id"],
                    "route_id": dirty_text(route["route_id"], rng), "planned_dispatch_timestamp": dirty_date(order_ts + timedelta(hours=12), rng),
                    "actual_dispatch_timestamp": dirty_date(dispatch_dt, rng), "shipment_status": rng.choice(["DELIVERED", "Delivered"]),
                    "total_weight_value": round(total_weight * (1000 if rng.random() < 0.08 else 1), 2),
                    "weight_uom": "G" if total_weight >= 0 and shipments and rng.random() < 0.08 else "KG",
                    "source_updated_at": iso(extracted_at),
                })
                # Correct any accidental UOM/value mismatch caused by separate random calls.
                if shipments[-1]["weight_uom"] == "G":
                    shipments[-1]["total_weight_value"] = round(total_weight * 1000, 2)
                else:
                    shipments[-1]["total_weight_value"] = round(total_weight, 3)
                for _, line_no, product_id, qty in order_fulfilled:
                    shipment_lines.append({
                        "shipment_id": shipment_id, "order_id": order_id, "order_line_no": line_no,
                        "product_id": dirty_text(product_id, rng), "shipped_qty": qty,
                        "shipment_uom": "EA", "source_updated_at": iso(extracted_at),
                    })
                event_base = order_ts + timedelta(hours=2)
                for sequence, (event_type, event_time) in enumerate([
                    ("PICK_START", event_base), (rng.choice(["PICK_COMPLETE", "Picked"]), event_base + timedelta(hours=rng.randint(1, 8))),
                    ("PACK_COMPLETE", event_base + timedelta(hours=rng.randint(9, 16))), ("DISPATCH", dispatch_dt)], 1):
                    wms_events.append({
                        "wms_event_id": f"WMS-{shipment_counter:08d}-{sequence}", "order_id": dirty_text(order_id, rng),
                        "shipment_id": shipment_id if event_type == "DISPATCH" else "", "location_id": location["location_id"],
                        "event_type": dirty_text(event_type, rng), "event_timestamp": dirty_date(event_time, rng),
                        "operator_shift": rng.choice(["A", "B", "C", "Night"]),
                        "quantity_processed": sum(x[3] for x in order_fulfilled), "exception_code": "" if rng.random() > 0.025 else "SHORT_PICK",
                        "source_updated_at": iso(extracted_at),
                    })
                for sequence, (event_type, event_time) in enumerate([
                    ("PICKED_UP", dispatch_dt), ("IN_TRANSIT", dispatch_dt + timedelta(hours=8)),
                    (rng.choice(["DELIVERED", "Delivered"]), delivery_dt)], 1):
                    delivery_events.append({
                        "delivery_event_id": f"DLE-{shipment_counter:08d}-{sequence}", "shipment_id": dirty_text(shipment_id, rng),
                        "event_sequence": sequence, "event_type": event_type, "event_timestamp": dirty_date(event_time, rng),
                        "event_location": region, "proof_of_delivery_flag": "Y" if sequence == 3 else "",
                        "source_updated_at": iso(extracted_at),
                    })
                freight = total_weight * carrier["base_rate_thb_per_kg"] * (1 + carrier["fuel_surcharge_pct"]) + route["toll_cost_thb"]
                for component, amount in [("FREIGHT", freight), ("WAREHOUSE_HANDLING", 18 + 2.2 * len(order_fulfilled)),
                                          ("FUEL_SURCHARGE", freight * carrier["fuel_surcharge_pct"])]:
                    costs.append({
                        "cost_id": f"CST-{len(costs) + 1:09d}", "shipment_id": dirty_text(shipment_id, rng),
                        "order_id": order_id, "cost_component": dirty_text(component, rng), "amount": round(amount, 2),
                        "currency_code": "THB", "posting_date": iso(min(end, delivery_dt.date())),
                        "source_updated_at": iso(extracted_at),
                    })

        # Weekly inventory extracts; blank safety stock emulates missing master data.
        if day_idx % 7 == 0 or current == end:
            for product in products:
                for location in locations:
                    product_id, location_id = product["product_id"], location["location_id"]
                    inventory_snapshots.append({
                        "snapshot_date": iso(current), "product_id": dirty_text(product_id, rng), "location_id": location_id,
                        "on_hand_qty": inventory[(product_id, location_id)], "reserved_qty": rng.randint(0, 15),
                        "safety_stock_qty": "" if rng.random() < 0.03 else reorder[(product_id, location_id)],
                        "inventory_uom": "EA", "source_updated_at": iso(extracted_at),
                    })

    # Demand/forecast extract is derived from the same operational demand.
    for day_idx in range(cfg["days"]):
        current = start + timedelta(days=day_idx)
        for product in products:
            for location in locations:
                key = (current, product["product_id"], location["location_id"])
                actual = actual_demand[key]
                if actual or rng.random() < 0.07:
                    forecast = max(0, round(actual * rng.uniform(0.72, 1.35) + rng.uniform(-2, 4)))
                    demand.append({
                        "demand_date": iso(current), "product_id": dirty_text(product["product_id"], rng),
                        "location_id": location["location_id"], "forecast_version": rng.choice(["BASELINE", "Baseline"]),
                        "forecast_qty": forecast, "actual_demand_qty": actual, "demand_uom": "EA",
                        "source_updated_at": iso(extracted_at),
                    })

    tables.update({
        "planning_demand_forecast": demand, "erp_sales_orders": orders, "erp_sales_order_lines": lines,
        "wms_inventory_snapshot": inventory_snapshots, "erp_purchase_orders": purchase_orders,
        "wms_purchase_order_receipts": receipts, "wms_activity_events": wms_events,
        "tms_shipments": shipments, "tms_shipment_lines": shipment_lines,
        "tms_delivery_events": delivery_events, "finance_logistics_costs": costs,
    })

    counts = {}
    duplicate_rate = 0.004
    for name, rows in tables.items():
        counts[name] = write_csv(output_dir / f"{name}.csv", rows, duplicate_rate, rng)

    manifest = {
        "generator_version": "1.0.0", "profile": profile, "seed": seed,
        "period_start": iso(start), "period_end": iso(end), "generated_at": iso(extracted_at),
        "table_count": len(tables), "row_counts_including_raw_duplicates": counts,
        "raw_quality_contract": {
            "exact_duplicate_rate": duplicate_rate,
            "known_issues": [
                "Business keys may contain leading/trailing spaces or inconsistent case",
                "Timestamps use ISO, DD/MM/YYYY and MM/DD/YYYY formats",
                "Statuses and event names use inconsistent labels and case",
                "Weights may be expressed in KG or G; order quantities use EA or Each",
                "Some optional safety-stock, shelf-life and exception values are null",
                "Purchase costs may be THB or USD and require an exchange-rate reference",
                "Late and partial supplier receipts, backorders and warehouse exceptions are valid business conditions",
            ],
        },
    }
    (output_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, default="dev")
    parser.add_argument("--seed", type=int, default=740561)
    parser.add_argument("--output", type=Path, default=Path("data/raw/dev"))
    args = parser.parse_args()
    manifest = generate(args.profile, args.seed, args.output)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
