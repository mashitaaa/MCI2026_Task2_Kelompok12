-- ============================================================
-- DDL: Pembuatan Database & Tabel di ClickHouse
-- Dataset: Instacart-style Orders API
-- ============================================================

CREATE DATABASE IF NOT EXISTS orders_db;

-- Tabel 1: Data per order (+ kolom hasil preprocessing)
CREATE TABLE IF NOT EXISTS orders_db.orders (
    order_id                UInt32,
    user_id                 UInt32,
    order_number            UInt32   COMMENT 'Urutan order ke-berapa dari user ini',
    order_dow               UInt8    COMMENT 'Hari dalam seminggu (0=Sabtu)',
    order_hour_of_day       UInt8    COMMENT 'Jam order dibuat (0-23)',
    days_since_prior_order  Float32  COMMENT 'Jarak hari dari order sebelumnya (0=order pertama)',
    eval_set                String   COMMENT 'Dataset split: prior/train/test',
    total_products          UInt32   COMMENT 'Jumlah produk dalam order ini',
    order_day_name          String   COMMENT 'Nama hari (feature engineering dari order_dow)',
    is_first_order          UInt8    COMMENT '1 jika order pertama user (feature engineering)',
    order_time_category     String   COMMENT 'morning/afternoon/evening/night (feature engineering)',
    ingested_at             DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, order_id)
COMMENT 'Data order dengan hasil preprocessing & feature engineering';

-- Tabel 2: Data produk per order
CREATE TABLE IF NOT EXISTS orders_db.order_products (
    order_id          UInt32,
    user_id           UInt32,
    product_id        UInt32,
    product_name      String,
    aisle_id          UInt32,
    aisle             String,
    department_id     UInt32,
    department        String,
    add_to_cart_order UInt32  COMMENT 'Urutan produk ditambahkan ke keranjang',
    reordered         UInt8   COMMENT '1 jika pernah dipesan sebelumnya',
    is_reordered      UInt8   COMMENT 'Feature engineering dari kolom reordered',
    ingested_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (order_id, product_id)
COMMENT 'Data produk per order (flattened dari nested array + preprocessing)';

-- ============================================================
-- VALIDASI
-- ============================================================
-- SELECT COUNT(*) FROM orders_db.orders;
-- SELECT COUNT(*) FROM orders_db.order_products;
-- SELECT * FROM orders_db.orders LIMIT 5;
-- SELECT * FROM orders_db.order_products LIMIT 5;

-- ============================================================
-- QUERY UNTUK METABASE
-- ============================================================

-- Q1: Total order per hari (Bar Chart)
SELECT order_day_name, COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY order_day_name
ORDER BY total_orders DESC;

-- Q2: Total order per waktu (morning/afternoon/evening/night)
SELECT order_time_category, COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY order_time_category
ORDER BY total_orders DESC;

-- Q3: First order vs Repeat order
SELECT
    CASE WHEN is_first_order = 1 THEN 'First Order' ELSE 'Repeat Order' END AS order_type,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY is_first_order;

-- Q4: Top 10 produk terlaris
SELECT product_name, department, COUNT(*) AS total_ordered
FROM orders_db.order_products
GROUP BY product_name, department
ORDER BY total_ordered DESC
LIMIT 10;

-- Q5: Top department berdasarkan jumlah produk terjual
SELECT department, COUNT(*) AS total_products_sold
FROM orders_db.order_products
GROUP BY department
ORDER BY total_products_sold DESC;

-- Q6: Reorder rate per department
SELECT
    department,
    COUNT(*) AS total_items,
    SUM(is_reordered) AS reordered_items,
    ROUND(SUM(is_reordered) * 100.0 / COUNT(*), 2) AS reorder_rate_pct
FROM orders_db.order_products
GROUP BY department
ORDER BY reorder_rate_pct DESC;
