#!/usr/bin/env python3
"""Fast structural checks for a generated raw dataset."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def norm(value: str) -> str:
    return value.strip().upper()


def load(folder: Path, name: str) -> list[dict[str, str]]:
    with (folder / f"{name}.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    # Bronze contains deliberate exact duplicates; validate the logical records.
    unique = {tuple(sorted(row.items())): row for row in rows}
    return list(unique.values())


def validate(folder: Path) -> dict:
    products = {norm(r["product_id"]) for r in load(folder, "erp_products")}
    customers = {norm(r["customer_id"]) for r in load(folder, "crm_customers")}
    orders = {norm(r["order_id"]) for r in load(folder, "erp_sales_orders")}
    shipments = {norm(r["shipment_id"]) for r in load(folder, "tms_shipments")}
    order_lines = load(folder, "erp_sales_order_lines")
    shipment_lines = load(folder, "tms_shipment_lines")

    allocated = {(norm(r["order_id"]), int(r["order_line_no"])): int(r["allocated_qty"])
                 for r in order_lines}
    shipped = defaultdict(int)
    for row in shipment_lines:
        shipped[(norm(row["order_id"]), int(row["order_line_no"]))] += int(row["shipped_qty"])

    failures = {
        "order_line_product_orphans": sum(norm(r["product_id"]) not in products for r in order_lines),
        "order_customer_orphans": sum(norm(r["customer_id"]) not in customers
                                      for r in load(folder, "erp_sales_orders")),
        "order_line_order_orphans": sum(norm(r["order_id"]) not in orders for r in order_lines),
        "shipment_order_orphans": sum(norm(r["order_id"]) not in orders
                                      for r in load(folder, "tms_shipments")),
        "delivery_shipment_orphans": sum(norm(r["shipment_id"]) not in shipments
                                           for r in load(folder, "tms_delivery_events")),
        "cost_shipment_orphans": sum(norm(r["shipment_id"]) not in shipments
                                      for r in load(folder, "finance_logistics_costs")),
        "allocated_greater_than_ordered": sum(int(r["allocated_qty"]) > int(r["ordered_qty"])
                                                for r in order_lines),
        "shipped_greater_than_allocated": sum(qty > allocated.get(key, -1) for key, qty in shipped.items()),
    }
    return {"status": "PASS" if not any(failures.values()) else "FAIL", "failures": failures}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path, nargs="?", default=Path("data/raw/dev"))
    args = parser.parse_args()
    result = validate(args.folder)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
