"""Build a volume-sized linked batch for the Azure event-driven path."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from src.generate_incremental_batch import HEADERS, rows


PROD_ORDER_COUNT = 1_600


def _clone(row: dict, replacements: dict[str, str], source_updated_at: str) -> dict:
    cloned = deepcopy(row)
    for column, value in list(cloned.items()):
        if isinstance(value, str):
            for old, new in replacements.items():
                value = value.replace(old, new)
            cloned[column] = value
    cloned["source_updated_at"] = source_updated_at
    return cloned


def build_event_rows(batch_date: str, batch_id: str) -> tuple[dict, str]:
    event_day = datetime.strptime(batch_date, "%Y/%m/%d")
    suffix = event_day.strftime("%Y%m%d")
    tag = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:6].upper()
    # Use the actual event-ingestion time so a new batch advances the Silver
    # watermark even when multiple demo batches share the same business date.
    source_updated_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
    template = deepcopy(rows(event_day.date(), source_updated_at))
    batch = {
        entity: deepcopy(template[entity])
        for entity in (
            "crm_customers", "erp_locations", "erp_products", "erp_suppliers",
            "tms_carriers", "tms_routes",
        )
    }
    for entity_rows in batch.values():
        for row in entity_rows:
            row["source_updated_at"] = source_updated_at

    transaction_entities = (
        "erp_purchase_orders", "erp_sales_order_lines", "erp_sales_orders",
        "finance_logistics_costs", "planning_demand_forecast", "risk_disruptions",
        "tms_delivery_events", "tms_shipment_lines", "tms_shipments",
        "wms_activity_events", "wms_inventory_snapshot", "wms_purchase_order_receipts",
    )
    batch.update({entity: [] for entity in transaction_entities})

    order_template = template["erp_sales_orders"][0]
    line_templates = template["erp_sales_order_lines"]
    shipment_template = template["tms_shipments"][0]
    shipment_line_templates = template["tms_shipment_lines"]
    delivery_templates = template["tms_delivery_events"]
    wms_templates = template["wms_activity_events"]

    # Match the local UAT development scale: approximately 1,600 orders and
    # 4,800 order lines per event batch. The default UAT generator is untouched.
    for index in range(1, PROD_ORDER_COUNT + 1):
        # Keep date-based fact keys unique so Silver MERGE retains the volume.
        unit_day = event_day + timedelta(days=index - 1)
        unit = f"{suffix}-{tag}-{index:05d}"
        # Silver contract: order_id must be exactly SO- followed by 8 digits.
        # Keep the batch tag in the numeric portion so IDs remain unique per batch.
        order_number = (int(tag, 16) * PROD_ORDER_COUNT + index) % 100_000_000
        order_id = f"SO-{order_number:08d}"
        shipment_id = f"SHP-EVT-{unit}"
        po_id = f"PO-EVT-{unit}"
        receipt_id = f"REC-EVT-{unit}"
        replacements = {
            "SO-00002001": order_id,
            "SHP-00002001": shipment_id,
            "PO-00009001": po_id,
            "REC-00009001": receipt_id,
            "DIS-009001": f"DIS-EVT-{unit}",
        }

        order = _clone(order_template, replacements, source_updated_at)
        order["order_timestamp"] = (unit_day + timedelta(hours=8, minutes=15)).isoformat()
        order["requested_delivery_date"] = (unit_day + timedelta(days=2)).date().isoformat()
        batch["erp_sales_orders"].append(order)

        lines = [_clone(row, replacements, source_updated_at) for row in line_templates]
        extra_line = _clone(line_templates[0], replacements, source_updated_at)
        extra_line.update({"order_line_no": "3", "ordered_qty": "8", "allocated_qty": "8"})
        lines.append(extra_line)
        batch["erp_sales_order_lines"].extend(lines)

        shipment = _clone(shipment_template, replacements, source_updated_at)
        shipment.update({
            "planned_dispatch_timestamp": (unit_day + timedelta(hours=16)).isoformat(),
            "actual_dispatch_timestamp": (unit_day + timedelta(hours=18)).isoformat(),
            "total_weight_value": "26.0",
        })
        batch["tms_shipments"].append(shipment)

        shipment_lines = [_clone(row, replacements, source_updated_at) for row in shipment_line_templates]
        extra_shipment_line = _clone(shipment_line_templates[0], replacements, source_updated_at)
        extra_shipment_line.update({"order_line_no": "3", "shipped_qty": "8"})
        batch["tms_shipment_lines"].extend(shipment_lines + [extra_shipment_line])

        for event_index, row in enumerate(delivery_templates, 1):
            event = _clone(row, replacements, source_updated_at)
            event["delivery_event_id"] = f"DLE-EVT-{unit}-{event_index}"
            event["event_timestamp"] = (unit_day + timedelta(hours=18 + 16 * (event_index - 1))).isoformat()
            batch["tms_delivery_events"].append(event)

        for event_index, row in enumerate(wms_templates, 1):
            event = _clone(row, replacements, source_updated_at)
            event["wms_event_id"] = f"WMS-EVT-{unit}-{event_index}"
            event["event_timestamp"] = (unit_day + timedelta(hours=8 + 3 * event_index)).isoformat()
            event["quantity_processed"] = "26"
            batch["wms_activity_events"].append(event)

        for cost_index, row in enumerate(template["finance_logistics_costs"], 1):
            cost = _clone(row, replacements, source_updated_at)
            cost["cost_id"] = f"CST-EVT-{unit}-{cost_index}"
            cost["posting_date"] = (unit_day + timedelta(days=2)).date().isoformat()
            batch["finance_logistics_costs"].append(cost)

        purchase_order = _clone(template["erp_purchase_orders"][0], replacements, source_updated_at)
        purchase_order["po_date"] = unit_day.date().isoformat()
        purchase_order["promised_date"] = (unit_day + timedelta(days=5)).date().isoformat()
        batch["erp_purchase_orders"].append(purchase_order)

        receipt = _clone(template["wms_purchase_order_receipts"][0], replacements, source_updated_at)
        receipt["receipt_timestamp"] = (unit_day + timedelta(hours=14)).isoformat()
        receipt["promised_date"] = purchase_order["promised_date"]
        batch["wms_purchase_order_receipts"].append(receipt)

        forecast = _clone(template["planning_demand_forecast"][0], replacements, source_updated_at)
        forecast["demand_date"] = unit_day.date().isoformat()
        batch["planning_demand_forecast"].append(forecast)

        disruption = _clone(template["risk_disruptions"][0], replacements, source_updated_at)
        disruption["start_date"] = unit_day.date().isoformat()
        disruption["end_date"] = (unit_day + timedelta(days=2)).date().isoformat()
        batch["risk_disruptions"].append(disruption)

        for row in template["wms_inventory_snapshot"]:
            inventory = _clone(row, replacements, source_updated_at)
            inventory["snapshot_date"] = unit_day.date().isoformat()
            batch["wms_inventory_snapshot"].append(inventory)

    return batch, source_updated_at


__all__ = ["HEADERS", "build_event_rows"]
