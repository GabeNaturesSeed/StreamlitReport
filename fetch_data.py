"""
Fetch WooCommerce order data and save to local parquet cache files.
Designed to run on a schedule via GitHub Actions.
"""

import os
import json
import time
import logging
from datetime import datetime, date

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WC_URL = os.getenv("WC_URL", "https://naturesseed.com/wp-json/wc/v3")
WC_KEY = os.getenv("WC_KEY")
WC_SECRET = os.getenv("WC_SECRET")
DEFAULT_MARGIN = float(os.getenv("DEFAULT_MARGIN", "0.5"))

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
COGS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Cost per SKU.csv")


def load_sku_costs() -> dict:
    """Load COGS lookup from CSV file."""
    if not os.path.exists(COGS_CSV):
        logger.warning("Cost per SKU.csv not found at %s — using meta/default only", COGS_CSV)
        return {}

    df = pd.read_csv(COGS_CSV)
    df.columns = [c.strip().lower() for c in df.columns]

    sku_col = next((c for c in df.columns if c in ("sku", "product_id")), None)
    cost_col = next((c for c in df.columns if c in ("unit cost", "cost", "cogs")), None)

    if sku_col is None or cost_col is None:
        logger.warning("CSV missing expected columns (found: %s). Skipping CSV COGS.", list(df.columns))
        return {}

    df[sku_col] = df[sku_col].astype(str).str.strip()
    df[cost_col] = pd.to_numeric(df[cost_col], errors="coerce")
    costs = dict(zip(df[sku_col], df[cost_col]))
    costs = {k: v for k, v in costs.items() if pd.notna(v)}
    logger.info("Loaded %d SKU costs from CSV", len(costs))
    return costs


def fetch_orders(date_after: str, date_before: str) -> list:
    """Fetch all orders in a date range from WooCommerce API with pagination."""
    all_orders = []
    page = 1
    while True:
        params = {
            "status": "processing,completed",
            "after": date_after,
            "before": date_before,
            "per_page": 100,
            "page": page,
            "orderby": "date",
            "order": "asc",
        }
        logger.info("Fetching orders page %d (after=%s, before=%s)", page, date_after, date_before)
        resp = requests.get(
            f"{WC_URL}/orders",
            params=params,
            auth=(WC_KEY, WC_SECRET),
            timeout=60,
        )
        resp.raise_for_status()
        orders = resp.json()
        if not orders:
            break
        all_orders.extend(orders)
        page += 1
        time.sleep(0.5)
    logger.info("Fetched %d orders total for range %s to %s", len(all_orders), date_after, date_before)
    return all_orders


def extract_line_items(orders: list, sku_costs: dict) -> tuple[pd.DataFrame, dict]:
    """Flatten orders into one-row-per-line-item DataFrame and track COGS coverage."""
    rows = []
    coverage = {
        "total_skus_in_orders": 0,
        "matched_from_csv": 0,
        "matched_from_meta": 0,
        "fallback_to_default": 0,
        "unmatched_skus": [],
    }
    seen_skus = set()

    for order in orders:
        order_id = order["id"]
        order_date = order["date_created"]
        customer_id = order.get("customer_id", 0)
        customer_email = order.get("billing", {}).get("email", "")

        for item in order.get("line_items", []):
            product_id = item.get("product_id", 0)
            product_name = item.get("name", "")
            sku = item.get("sku", "") or ""
            quantity = item.get("quantity", 0)
            line_total = float(item.get("subtotal", 0))

            # Category: first category name if available
            categories = item.get("categories", [])
            if not categories:
                # Try product meta for category
                category = "Uncategorized"
            else:
                category = categories[0].get("name", "Uncategorized") if categories else "Uncategorized"

            # COGS lookup (priority order)
            cogs = None
            cogs_source = None

            # 1. CSV lookup by SKU
            if sku and sku in sku_costs:
                cogs = sku_costs[sku] * quantity
                cogs_source = "csv"

            # 2. Meta _cogs
            if cogs is None:
                meta = item.get("meta_data", [])
                for m in meta:
                    if m.get("key") == "_cogs":
                        try:
                            cogs = float(m["value"]) * quantity
                            cogs_source = "meta"
                        except (ValueError, TypeError):
                            pass
                        break

            # 3. Default margin fallback
            if cogs is None:
                cogs = line_total * (1 - DEFAULT_MARGIN)
                cogs_source = "default"
                if sku:
                    logger.warning("No COGS found for SKU: %s, falling back to default margin", sku)

            gross_margin = line_total - cogs

            # Track coverage
            if sku and sku not in seen_skus:
                seen_skus.add(sku)
                coverage["total_skus_in_orders"] += 1
                if cogs_source == "csv":
                    coverage["matched_from_csv"] += 1
                elif cogs_source == "meta":
                    coverage["matched_from_meta"] += 1
                else:
                    coverage["fallback_to_default"] += 1
                    coverage["unmatched_skus"].append(sku)

            rows.append({
                "order_id": order_id,
                "order_date": order_date,
                "customer_id": customer_id,
                "customer_email": customer_email,
                "product_id": product_id,
                "product_name": product_name,
                "sku": sku,
                "category": category,
                "quantity": quantity,
                "line_total": line_total,
                "cogs": cogs,
                "gross_margin": gross_margin,
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["order_date"] = pd.to_datetime(df["order_date"])
    return df, coverage


def build_customer_first_order(orders_cy: list, orders_ly: list) -> pd.DataFrame:
    """Determine each customer's first-ever order date from all fetched orders."""
    all_orders = orders_cy + orders_ly
    records = []
    for order in all_orders:
        cid = order.get("customer_id", 0)
        email = order.get("billing", {}).get("email", "")
        dt = order.get("date_created", "")
        if cid or email:
            records.append({
                "customer_id": cid,
                "customer_email": email,
                "order_date": dt,
            })

    if not records:
        return pd.DataFrame(columns=["customer_id", "first_order_date"])

    df = pd.DataFrame(records)
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Group by customer_id (prefer) or email
    df["customer_key"] = df.apply(
        lambda r: str(r["customer_id"]) if r["customer_id"] else r["customer_email"], axis=1
    )
    first_orders = df.groupby("customer_key")["order_date"].min().reset_index()
    first_orders.columns = ["customer_key", "first_order_date"]

    # Map back to customer_id
    key_to_id = df.drop_duplicates("customer_key")[["customer_key", "customer_id"]].set_index("customer_key")["customer_id"]
    first_orders["customer_id"] = first_orders["customer_key"].map(key_to_id)
    return first_orders[["customer_id", "first_order_date"]]


def main():
    os.makedirs(CACHE_DIR, exist_ok=True)

    today = date.today()
    cy_start = f"{today.year}-01-01T00:00:00"
    cy_end = today.isoformat() + "T23:59:59"
    ly_start = f"{today.year - 1}-01-01T00:00:00"
    ly_end = f"{today.year - 1}-12-31T23:59:59"

    sku_costs = load_sku_costs()

    logger.info("Fetching current year orders...")
    orders_cy_raw = fetch_orders(cy_start, cy_end)

    logger.info("Fetching last year orders...")
    orders_ly_raw = fetch_orders(ly_start, ly_end)

    logger.info("Extracting line items for CY...")
    df_cy, coverage_cy = extract_line_items(orders_cy_raw, sku_costs)

    logger.info("Extracting line items for LY...")
    df_ly, coverage_ly = extract_line_items(orders_ly_raw, sku_costs)

    # Merge coverage stats
    coverage = {
        "total_skus_in_orders": coverage_cy["total_skus_in_orders"] + coverage_ly["total_skus_in_orders"],
        "matched_from_csv": coverage_cy["matched_from_csv"] + coverage_ly["matched_from_csv"],
        "matched_from_meta": coverage_cy["matched_from_meta"] + coverage_ly["matched_from_meta"],
        "fallback_to_default": coverage_cy["fallback_to_default"] + coverage_ly["fallback_to_default"],
        "unmatched_skus": list(set(coverage_cy["unmatched_skus"] + coverage_ly["unmatched_skus"])),
    }

    logger.info("Building customer first-order map...")
    df_customers = build_customer_first_order(orders_cy_raw, orders_ly_raw)

    # Save outputs
    if not df_cy.empty:
        df_cy.to_parquet(os.path.join(CACHE_DIR, "orders_cy.parquet"), index=False)
        logger.info("Saved %d CY line items", len(df_cy))
    else:
        logger.warning("No CY orders found")

    if not df_ly.empty:
        df_ly.to_parquet(os.path.join(CACHE_DIR, "orders_ly.parquet"), index=False)
        logger.info("Saved %d LY line items", len(df_ly))
    else:
        logger.warning("No LY orders found")

    if not df_customers.empty:
        df_customers.to_parquet(os.path.join(CACHE_DIR, "customer_first_order.parquet"), index=False)
        logger.info("Saved %d customer records", len(df_customers))

    with open(os.path.join(CACHE_DIR, "cogs_coverage.json"), "w") as f:
        json.dump(coverage, f, indent=2)
    logger.info("COGS coverage: %s", {k: v for k, v in coverage.items() if k != "unmatched_skus"})

    meta = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "cy_period": f"Jan 1 - {today.strftime('%b %d, %Y')}",
        "ly_period": f"Jan 1 - Dec 31, {today.year - 1}",
    }
    with open(os.path.join(CACHE_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    logger.info("Done. Cache updated at %s", meta["last_updated"])


if __name__ == "__main__":
    main()
