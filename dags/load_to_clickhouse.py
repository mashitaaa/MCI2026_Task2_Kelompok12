"""
load_to_clickhouse.py
Task 2 DAG: Baca JSON dari data lake → load ke ClickHouse → hapus file JSON.
"""

import json
import os
import glob
import logging
from datetime import datetime
from clickhouse_driver import Client

CLICKHOUSE_HOST     = "clickhouse-server"
CLICKHOUSE_PORT     = 9000
CLICKHOUSE_USER     = "admin"
CLICKHOUSE_PASSWORD = "rahasia"
CLICKHOUSE_DB       = "orders_db"
DATA_LAKE_PATH      = "/opt/airflow/data_lake"

logger = logging.getLogger(__name__)


def get_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def setup_tables(client):
    """Buat database dan tabel jika belum ada."""
    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    # Tabel orders (dengan kolom hasil preprocessing)
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders (
            order_id                UInt32,
            user_id                 UInt32,
            order_number            UInt32,
            order_dow               UInt8,
            order_hour_of_day       UInt8,
            days_since_prior_order  Float32,
            eval_set                String,
            total_products          UInt32,
            order_day_name          String  COMMENT 'Nama hari hasil feature engineering',
            is_first_order          UInt8   COMMENT '1 jika order pertama user',
            order_time_category     String  COMMENT 'morning/afternoon/evening/night',
            ingested_at             DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (user_id, order_id)
    """)

    # Tabel order_products (dengan kolom hasil preprocessing)
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.order_products (
            order_id          UInt32,
            user_id           UInt32,
            product_id        UInt32,
            product_name      String,
            aisle_id          UInt32,
            aisle             String,
            department_id     UInt32,
            department        String,
            add_to_cart_order UInt32,
            reordered         UInt8,
            is_reordered      UInt8  COMMENT 'Feature engineering dari kolom reordered',
            ingested_at       DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
    """)
    logger.info("Struktur tabel siap.")


def load_to_clickhouse():
    logger.info("=" * 60)
    logger.info("Memulai proses load ke ClickHouse...")

    try:
        client = get_client()
        setup_tables(client)
    except Exception as e:
        logger.error(f"Gagal koneksi ClickHouse: {e}")
        raise

    # Ambil semua file JSON di data lake
    files = glob.glob(os.path.join(DATA_LAKE_PATH, "*.json"))
    if not files:
        logger.warning("Tidak ada file JSON ditemukan.")
        return

    logger.info(f"Ditemukan {len(files)} file JSON.")

    order_rows   = []
    product_rows = []
    now          = datetime.now()

    for filepath in files:
        logger.info(f"Membaca: {filepath}")
        with open(filepath, "r") as f:
            orders_list = json.load(f)

        if isinstance(orders_list, dict):
            orders_list = orders_list.get("orders", [])

        for o in orders_list:
            # Row untuk tabel orders
            order_rows.append((
                int(o.get("order_id") or 0),
                int(o.get("user_id") or 0),
                int(o.get("order_number") or 0),
                int(o.get("order_dow") or 0),
                int(o.get("order_hour_of_day") or 0),
                float(o.get("days_since_prior_order") or 0.0),
                str(o.get("eval_set") or "prior"),
                int(o.get("total_products") or 0),
                str(o.get("order_day_name") or "unknown"),
                int(o.get("is_first_order") or 0),
                str(o.get("order_time_category") or "unknown"),
                now,
            ))

            # Row untuk tabel order_products
            for p in o.get("products", []):
                product_rows.append((
                    int(o.get("order_id") or 0),
                    int(o.get("user_id") or 0),
                    int(p.get("product_id") or 0),
                    str(p.get("product_name") or "unknown"),
                    int(p.get("aisle_id") or 0),
                    str(p.get("aisle") or "unknown"),
                    int(p.get("department_id") or 0),
                    str(p.get("department") or "unknown"),
                    int(p.get("add_to_cart_order") or 0),
                    int(p.get("reordered") or 0),
                    int(p.get("is_reordered") or 0),
                    now,
                ))

    # Truncate & Insert
    try:
        client.execute(f"TRUNCATE TABLE {CLICKHOUSE_DB}.orders")
        client.execute(f"TRUNCATE TABLE {CLICKHOUSE_DB}.order_products")

        if order_rows:
            client.execute(
                f"INSERT INTO {CLICKHOUSE_DB}.orders "
                f"(order_id, user_id, order_number, order_dow, order_hour_of_day, "
                f"days_since_prior_order, eval_set, total_products, order_day_name, "
                f"is_first_order, order_time_category, ingested_at) VALUES",
                order_rows
            )
            logger.info(f"✅ {len(order_rows)} baris masuk ke tabel orders.")

        if product_rows:
            client.execute(
                f"INSERT INTO {CLICKHOUSE_DB}.order_products "
                f"(order_id, user_id, product_id, product_name, aisle_id, aisle, "
                f"department_id, department, add_to_cart_order, reordered, "
                f"is_reordered, ingested_at) VALUES",
                product_rows
            )
            logger.info(f"✅ {len(product_rows)} baris masuk ke tabel order_products.")

    except Exception as e:
        logger.error(f"Gagal insert ke ClickHouse: {e}")
        raise

    # Hapus file JSON setelah berhasil diproses
    for filepath in files:
        try:
            os.remove(filepath)
            logger.info(f"🗑️  Hapus: {filepath}")
        except OSError as e:
            logger.warning(f"Gagal hapus {filepath}: {e}")

    logger.info("=== ETL LOAD TO CLICKHOUSE BERHASIL ===")
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_to_clickhouse()
