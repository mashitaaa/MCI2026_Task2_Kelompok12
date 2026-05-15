-- ============================================================
-- QUERY METABASE - orders_db (Instacart-style dataset)
-- ============================================================


-- Q1: Overview Metrics (Number cards)
-- Visualisasi: 4 Number cards
SELECT
    COUNT(DISTINCT o.order_id)   AS total_orders,
    COUNT(DISTINCT o.user_id)    AS total_users,
    COUNT(*)                     AS total_products_ordered,
    ROUND(AVG(o.total_products), 1) AS avg_products_per_order
FROM orders_db.orders o;


-- Q2: Distribusi Order per Hari dalam Seminggu
-- Visualisasi: Bar Chart
-- Insight: Hari apa pelanggan paling banyak belanja?
SELECT
    CASE order_dow
        WHEN 0 THEN 'Minggu'
        WHEN 1 THEN 'Senin'
        WHEN 2 THEN 'Selasa'
        WHEN 3 THEN 'Rabu'
        WHEN 4 THEN 'Kamis'
        WHEN 5 THEN 'Jumat'
        WHEN 6 THEN 'Sabtu'
    END AS hari,
    order_dow,
    COUNT(*) AS jumlah_order
FROM orders_db.orders
GROUP BY order_dow, hari
ORDER BY order_dow;


-- Q3: Distribusi Order per Jam
-- Visualisasi: Line Chart
-- Insight: Jam berapa peak order terjadi?
SELECT
    order_hour_of_day AS jam,
    COUNT(*) AS jumlah_order
FROM orders_db.orders
GROUP BY jam
ORDER BY jam;


-- Q4: Top 10 Departemen Terpopuler
-- Visualisasi: Bar Chart horizontal
-- Insight: Departemen produk mana yang paling banyak dipesan?
SELECT
    department,
    COUNT(*) AS total_items_ordered,
    COUNT(DISTINCT order_id) AS jumlah_order
FROM orders_db.order_products
GROUP BY department
ORDER BY total_items_ordered DESC
LIMIT 10;


-- Q5: Top 10 Produk Terlaris
-- Visualisasi: Bar Chart horizontal
-- Insight: Produk apa yang paling sering masuk keranjang?
SELECT
    product_name,
    department,
    aisle,
    COUNT(*) AS frekuensi_dipesan,
    SUM(reordered) AS total_reorder
FROM orders_db.order_products
GROUP BY product_name, department, aisle
ORDER BY frekuensi_dipesan DESC
LIMIT 10;


-- Q6: Top 10 Aisle Terpopuler
-- Visualisasi: Bar Chart
-- Insight: Lorong (kategori) mana yang paling ramai?
SELECT
    aisle,
    department,
    COUNT(*) AS total_items
FROM orders_db.order_products
GROUP BY aisle, department
ORDER BY total_items DESC
LIMIT 10;


-- Q7: Reorder Rate per Departemen
-- Visualisasi: Bar Chart
-- Insight: Produk departemen mana yang paling sering di-reorder?
SELECT
    department,
    COUNT(*) AS total_ordered,
    SUM(reordered) AS total_reordered,
    ROUND(SUM(reordered) * 100.0 / COUNT(*), 1) AS reorder_rate_pct
FROM orders_db.order_products
GROUP BY department
ORDER BY reorder_rate_pct DESC;


-- Q8: Distribusi Jumlah Produk per Order
-- Visualisasi: Bar Chart / Histogram
-- Insight: Rata-rata berapa produk per sekali order?
SELECT
    total_products AS jumlah_produk,
    COUNT(*) AS jumlah_order
FROM orders_db.orders
GROUP BY total_products
ORDER BY total_products;


-- Q9: User paling aktif (Top 10)
-- Visualisasi: Table
-- Insight: Siapa pelanggan paling sering order?
SELECT
    user_id,
    COUNT(DISTINCT order_id) AS total_order,
    MAX(order_number)        AS order_terakhir_ke,
    ROUND(AVG(days_since_prior_order), 1) AS rata_hari_antar_order,
    SUM(total_products)      AS total_produk_dibeli
FROM orders_db.orders
GROUP BY user_id
ORDER BY total_order DESC
LIMIT 10;
