"""
fetch_orders.py
Task 1 DAG: Fetch data dari API orders, preprocessing, simpan ke data_lake sebagai JSON.
"""

import requests
import json
import os
import logging
from datetime import datetime

API_URL        = "http://96.9.212.102:8000/orders"
DATA_LAKE_PATH = "/opt/airflow/data_lake"

logger = logging.getLogger(__name__)

# Mapping order_dow ke nama hari (0=Sabtu, 1=Minggu, dst - Instacart convention)
DOW_MAP = {0: "Saturday", 1: "Sunday", 2: "Monday", 3: "Tuesday",
           4: "Wednesday", 5: "Thursday", 6: "Friday"}


def preprocess_order(order):
    """Preprocessing satu order beserta produk-produknya."""

    # ── 1. Handle missing values ──────────────────────────────────────────
    days_since = order.get("days_since_prior_order")
    if days_since is None:
        days_since = 0.0   # order pertama user tidak punya prior order
    days_since = float(days_since)

    order_dow         = int(order.get("order_dow") or 0)
    order_hour        = int(order.get("order_hour_of_day") or 0)
    order_number      = int(order.get("order_number") or 0)

    # ── 2. Feature engineering ────────────────────────────────────────────
    # Nama hari dari dow
    order_day_name = DOW_MAP.get(order_dow, "unknown")

    # Apakah order pertama user
    is_first_order = 1 if days_since == 0.0 else 0

    # Kategori waktu order
    if 5 <= order_hour < 12:
        order_time_category = "morning"
    elif 12 <= order_hour < 17:
        order_time_category = "afternoon"
    elif 17 <= order_hour < 21:
        order_time_category = "evening"
    else:
        order_time_category = "night"

    # ── 3. Preprocessing produk ───────────────────────────────────────────
    clean_products = []
    for p in order.get("products", []):
        product_name = p.get("product_name") or ""
        # Filter: skip produk tanpa nama
        if not product_name.strip():
            continue

        clean_products.append({
            "product_id":        int(p.get("product_id") or 0),
            "product_name":      product_name.strip(),
            "aisle_id":          int(p.get("aisle_id") or 0),
            "aisle":             (p.get("aisle") or "unknown").strip(),
            "department_id":     int(p.get("department_id") or 0),
            "department":        (p.get("department") or "unknown").strip(),
            "add_to_cart_order": int(p.get("add_to_cart_order") or 0),
            "reordered":         int(p.get("reordered") or 0),
            # Feature engineering produk
            "is_reordered":      1 if int(p.get("reordered") or 0) == 1 else 0,
        })

    return {
        # Kolom asli
        "order_id":               int(order.get("order_id") or 0),
        "user_id":                int(order.get("user_id") or 0),
        "order_number":           order_number,
        "order_dow":              order_dow,
        "order_hour_of_day":      order_hour,
        "days_since_prior_order": days_since,
        "eval_set":               str(order.get("eval_set") or "prior"),
        "total_products":         len(clean_products),
        # Kolom baru hasil feature engineering
        "order_day_name":         order_day_name,
        "is_first_order":         is_first_order,
        "order_time_category":    order_time_category,
        # Produk yang sudah bersih
        "products":               clean_products,
    }


def fetch_orders():
    logger.info("=" * 60)
    logger.info("Memulai fetch data dari API Orders...")

    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        logger.info(f"API Response: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal fetch API: {e}")
        raise

    data   = response.json()
    orders = data.get("orders", [])
    logger.info(f"Total orders diterima dari API: {len(orders)}")

    # ── Preprocessing semua order ─────────────────────────────────────────
    clean_orders = [preprocess_order(o) for o in orders]
    logger.info(f"Total orders setelah preprocessing: {len(clean_orders)}")

    # ── Simpan ke data lake ───────────────────────────────────────────────
    os.makedirs(DATA_LAKE_PATH, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(DATA_LAKE_PATH, f"orders_{timestamp}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(clean_orders, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Data tersimpan ke: {output_path}")
    logger.info("=" * 60)
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_orders()
