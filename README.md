# MCI2026 Task 2 — Pipeline Orchestration & Data Visualization
**Kelompok 12 | Modul 2 & 3**

---

## Daftar Isi

1. [Gambaran Umum](#gambaran-umum)
2. [Arsitektur Pipeline](#arsitektur-pipeline)
3. [Struktur Repository](#struktur-repository)
4. [Prasyarat & Instalasi](#prasyarat--instalasi)
5. [Langkah 1 — Apache Airflow DAG](#langkah-1--apache-airflow-dag)
6. [Langkah 2 — Manajemen Data di ClickHouse](#langkah-2--manajemen-data-di-clickhouse)
7. [Langkah 3 — Visualisasi & Questions di Metabase](#langkah-3--visualisasi--questions-di-metabase)
8. [Langkah 4 — Membangun Dashboard di Metabase](#langkah-4--membangun-dashboard-di-metabase)
9. [Kendala & Solusi](#kendala--solusi)

---

## Gambaran Umum

Proyek ini merupakan implementasi **Pipeline Orchestration & Data Visualization** menggunakan stack berikut:

| Komponen | Teknologi |
|---|---|
| Orkestrasi Pipeline | Apache Airflow 2.9.0 |
| Columnar Database | ClickHouse 23.8 |
| Visualisasi & Dashboard | Metabase |
| Sumber Data | REST API `http://96.9.212.102:8000/orders` |
| Containerization | Docker & Docker Compose |

---

## Arsitektur Pipeline

```
┌────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                  │
│                                                        │
│   ┌─────────────┐           ┌──────────────────────┐   │
│   │ fetch_orders│ ────────► │ load_to_clickhouse   │   │
│   │  (Task 1)   │           │      (Task 2)        │   │
│   └─────────────┘           └──────────────────────┘   │
│         │                            │                 │
│    Fetch API &                  Read JSON &            │
│    Preprocessing               Insert ke ClickHouse    │
└────────────────────────────────────────────────────────┘
         │                            │
         ▼                            ▼
   ┌──────────┐               ┌──────────────┐
   │ data_lake│               │  ClickHouse  │
   │ (JSON)   │               │  orders_db   │
   └──────────┘               └──────────────┘
                                      │
                                      ▼
                               ┌──────────────┐
                               │   Metabase   │
                               │  Dashboard   │
                               └──────────────┘
```

Secara singkat, pipeline berjalan setiap **10 menit** secara otomatis:
1. Task `fetch_orders` mengambil data dari API, melakukan preprocessing & feature engineering, lalu menyimpan hasilnya sebagai file JSON ke folder `data_lake`.
2. Task `load_to_clickhouse` membaca file JSON tersebut, kemudian memasukkan data ke tabel-tabel di ClickHouse. File JSON dihapus setelah berhasil diproses.
3. Metabase terkoneksi ke ClickHouse dan memvisualisasikan data secara real-time.

---

## Struktur Repository

```
MCI2026_Task2_Kelompok12/
├── dags/
│   ├── orders_pipeline.py       # Definisi DAG Airflow
│   ├── fetch_orders.py          # Task 1: Fetch API + Preprocessing
│   └── load_to_clickhouse.py    # Task 2: Load data ke ClickHouse
├── sql/
│   ├── ddl_orders.sql           # DDL: CREATE DATABASE & TABLE
│   └── queries_metabase.sql     # Kumpulan query untuk Metabase
├── data_lake/                   # Folder staging file JSON (auto-generated)
├── metabase_plugins/            # Plugin driver ClickHouse untuk Metabase
├── docker-compose.yml           # Orkestrasi seluruh layanan Docker
├── Dockerfile                   # Custom image Airflow + dependencies
├── requirements.txt             # Python dependencies
└── README.md
```

---

## Prasyarat & Instalasi

### Prasyarat

- Docker Desktop (v20+) & Docker Compose (v2+)
- Minimal RAM: 4 GB tersedia untuk Docker
- Port `8080`, `8123`, `9000`, dan `3000` tidak sedang digunakan

### Cara Menjalankan

**1. Clone repository**
```bash
git clone https://github.com/<username>/MCI2026_Task2_Kelompok12.git
cd MCI2026_Task2_Kelompok12
```

**2. Jalankan semua layanan**
```bash
docker compose up -d
```

**3. Tunggu hingga semua container berjalan (±2–3 menit), lalu cek statusnya**
```bash
docker compose ps
```

Pastikan semua service berstatus `healthy` atau `running`.

> <img width="477" height="131" alt="image" src="https://github.com/user-attachments/assets/52e5a104-afcc-4162-9d75-e3ed97680d71" />

**4. Akses layanan**

| Layanan | URL | Kredensial |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| ClickHouse HTTP | http://localhost:8123 | `admin` / `rahasia` |
| Metabase | http://localhost:3000 | (setup awal saat pertama buka) |

---

## Langkah 1 — Apache Airflow DAG

### 1.1 Struktur DAG

DAG `orders_pipeline` didefinisikan di `dags/orders_pipeline.py` dan terdiri dari dua task yang berjalan secara berurutan:

```
fetch_orders  ──►  load_to_clickhouse
```

DAG dikonfigurasi dengan `schedule_interval="*/10 * * * *"` (setiap 10 menit) dan `catchup=False` agar tidak memproses ulang run yang terlewat.

### 1.2 Task 1: `fetch_orders`

File: `dags/fetch_orders.py`

Task ini bertanggung jawab untuk:
- Mengambil data dari endpoint `http://96.9.212.102:8000/orders`
- Melakukan **preprocessing** setiap order (handle missing values, type casting)
- Melakukan **feature engineering**:
  - `order_day_name` — konversi angka hari ke nama hari (Sabtu, Minggu, dst.)
  - `is_first_order` — flag apakah ini order pertama user (berdasarkan `days_since_prior_order == 0`)
  - `order_time_category` — kategorisasi waktu menjadi `morning`, `afternoon`, `evening`, atau `night`
  - `is_reordered` — flag produk yang pernah dipesan sebelumnya
- Menyimpan hasil ke `data_lake/orders_<timestamp>.json`

```python
# Contoh logika feature engineering
if 5 <= order_hour < 12:
    order_time_category = "morning"
elif 12 <= order_hour < 17:
    order_time_category = "afternoon"
elif 17 <= order_hour < 21:
    order_time_category = "evening"
else:
    order_time_category = "night"
```

### 1.3 Task 2: `load_to_clickhouse`

File: `dags/load_to_clickhouse.py`

Task ini bertanggung jawab untuk:
- Membaca semua file JSON dari folder `data_lake`
- Memisahkan data menjadi dua list: `order_rows` dan `product_rows`
- Melakukan **TRUNCATE** lalu **INSERT** ke tabel ClickHouse
- Menghapus file JSON setelah proses berhasil

> <img width="486" height="354" alt="image" src="https://github.com/user-attachments/assets/b28667af-c04e-4b8c-b26d-8310047a4f30" />

---

## Langkah 2 — Manajemen Data di ClickHouse

### 2.1 Membuat Database & Tabel

DDL lengkap tersedia di `sql/ddl_orders.sql`. Tabel dibuat otomatis oleh task `load_to_clickhouse`, namun bisa juga dijalankan manual via HTTP API ClickHouse.

**Database:**
```sql
CREATE DATABASE IF NOT EXISTS orders_db;
```

**Tabel `orders`** — menyimpan data per transaksi:
```sql
CREATE TABLE IF NOT EXISTS orders_db.orders (
    order_id                UInt32,
    user_id                 UInt32,
    order_number            UInt32,
    order_dow               UInt8,
    order_hour_of_day       UInt8,
    days_since_prior_order  Float32,
    eval_set                String,
    total_products          UInt32,
    order_day_name          String,   -- feature engineering
    is_first_order          UInt8,    -- feature engineering
    order_time_category     String,   -- feature engineering
    ingested_at             DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, order_id);
```

**Tabel `order_products`** menyimpan detail produk per transaksi (hasil flatten dari nested array):
```sql
CREATE TABLE IF NOT EXISTS orders_db.order_products (
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
    is_reordered      UInt8,   -- feature engineering
    ingested_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (order_id, product_id);
```

**Alasan memilih MergeTree Engine:** MergeTree adalah engine default ClickHouse yang dioptimalkan untuk workload analitik dengan volume data besar, mendukung operasi INSERT massal yang efisien, serta query agregasi yang cepat.

### 2.2 Verifikasi Data Berhasil Di-load

Setelah pipeline berjalan setidaknya satu kali, lakukan validasi langsung melalui ClickHouse CLI untuk memastikan data sudah masuk dengan benar.

**Masuk ke container ClickHouse:**
```bash
docker exec -it mci2026_task2_kelompok12-clickhouse-server-1 clickhouse-client --user admin --password rahasia
```

> Nama container bisa berbeda tergantung nama folder project. Jika gagal, cek nama container yang benar terlebih dahulu dengan `docker compose ps`.

**Cek database yang tersedia:**
```sql
SHOW DATABASES;
```

**Masuk ke database dan cek struktur tabel:**
```sql
USE orders_db;

SHOW TABLES;

DESCRIBE orders_db.orders;
DESCRIBE orders_db.order_products;
```

**Validasi jumlah data yang berhasil masuk:**
```sql
SELECT COUNT(*) FROM orders_db.orders;
SELECT COUNT(*) FROM orders_db.order_products;
```

**Preview data orders (termasuk kolom hasil feature engineering):**
```sql
SELECT
    order_id,
    user_id,
    order_day_name,
    order_time_category,
    is_first_order,
    ingested_at
FROM orders_db.orders
LIMIT 10;
```

**Preview data order_products:**
```sql
SELECT
    order_id,
    product_name,
    department,
    is_reordered
FROM orders_db.order_products
LIMIT 10;
```

**Keluar dari ClickHouse CLI:**
```sql
exit
```

> <img width="218" height="335" alt="image" src="https://github.com/user-attachments/assets/6ab3ce75-e7e7-4583-8a69-831650f94d55" />

> <img width="943" height="440" alt="image" src="https://github.com/user-attachments/assets/273c1ea4-5cf5-4b23-a759-c3345a55e52f" />

> <img width="944" height="442" alt="image" src="https://github.com/user-attachments/assets/8aae9b70-ffd5-430f-8f9c-c6538d091777" />

---

## Langkah 3 — Visualisasi & Questions di Metabase

### 3.1 Koneksi Metabase ke ClickHouse

**Setup awal Metabase (hanya dilakukan sekali saat pertama kali buka):**

1. Buka Metabase di `http://localhost:3000`
2. Isi data diri untuk membuat akun admin (boleh menggunakan data dummy)
3. Pada halaman **Add your data**, pilih **ClickHouse** sebagai tipe database
4. Isi konfigurasi koneksi sebagai berikut:

| Field | Value |
|---|---|
| Database type | ClickHouse |
| Display name | Orders Data Warehouse |
| Host | `clickhouse-server` |
| Port | `8123` |
| Database name | `orders_db` |
| Username | `admin` |
| Password | `rahasia` |

5. Klik **Save** — Metabase akan melakukan tes koneksi secara otomatis
6. Jika koneksi berhasil, database `orders_db` akan muncul di daftar sumber data

> <img width="500" height="106" alt="image" src="https://github.com/user-attachments/assets/76504c08-d769-43a9-a8bb-8e282dca325b" />

> <img width="537" height="115" alt="image" src="https://github.com/user-attachments/assets/2fae79b9-22dc-4c07-a7b7-49d32b94715d" />

**Jika sudah pernah setup sebelumnya**, koneksi baru bisa ditambahkan melalui:
**Settings → Admin Settings → Databases → Add a database**

### 3.2 Membuat Questions (Queries)

Setiap question dibuat melalui menu **New → Question → Native Query** di Metabase.

---

#### Question 1: Total Order per Hari

Menampilkan distribusi total order berdasarkan hari dalam seminggu.

```sql
SELECT
    o.order_day_name        AS day_name,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM orders_db.orders o
GROUP BY
    o.order_dow,
    o.order_day_name
ORDER BY
    o.order_dow;
```

**Visualisasi:** Bar Chart dengan sumbu X: `day_name` dan sumbu Y: `total_orders`

> <img width="217" height="189" alt="image" src="https://github.com/user-attachments/assets/cb4eb8fd-9e1e-4e24-89db-3338587e1eee" />

---

#### Question 2: Top 10 Produk Terlaris

Menampilkan 10 produk yang paling banyak dipesan beserta kategori departemennya.

```sql
SELECT product_name, department, COUNT(*) AS total_ordered
FROM orders_db.order_products
GROUP BY product_name, department
ORDER BY total_ordered DESC
LIMIT 10;
```

**Visualisasi:** Bar Chart (horizontal) dengan sumbu Y: `product_name` dan sumbu X: `total_ordered`

> <img width="232" height="233" alt="image" src="https://github.com/user-attachments/assets/1df4629e-a0f8-4569-8fa9-883dfa96c802" />

---

#### Question 3: Top Department

Menampilkan kategori departemen yang paling banyak menghasilkan penjualan produk.

```sql
SELECT department, COUNT(*) AS total_products_sold
FROM orders_db.order_products
GROUP BY department
ORDER BY total_products_sold DESC;
```

**Visualisasi:** Pie Chart atau Bar Chart menunjukkan proporsi penjualan per departemen

> <img width="727" height="308" alt="image" src="https://github.com/user-attachments/assets/50850418-fea7-4203-af62-d8fc9445bb93" />

---

#### Question 4: Reorder Rate per Department

Mengukur tingkat loyalitas pelanggan per kategori produk berdasarkan persentase produk yang dipesan ulang.

```sql
SELECT
    department AS product_category,
    COUNT(*) AS total_items_purchased,
    SUM(is_reordered) AS total_reordered,
    ROUND(SUM(is_reordered) * 100.0 / COUNT(*), 2) AS reorder_rate_pct
FROM orders_db.order_products
GROUP BY department
ORDER BY reorder_rate_pct DESC;
```

**Visualisasi:** Bar Chart dengan sumbu X: `product_category` dan sumbu Y: `reorder_rate_pct`

> <img width="429" height="346" alt="image" src="https://github.com/user-attachments/assets/79cc79d3-39a4-41bf-b2c1-3c4f5f13fc39" />

---

#### Question 5: First Order vs Repeat Order

Membandingkan jumlah pelanggan baru (first order) dengan pelanggan yang sudah pernah berbelanja sebelumnya (repeat order).

```sql
SELECT
    CASE WHEN is_first_order = 1 THEN 'First Order' ELSE 'Repeat Order' END AS order_type,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY is_first_order;
```

**Visualisasi:** Pie Chart dengan proporsi antara `First Order` dan `Repeat Order`

> <img width="348" height="234" alt="image" src="https://github.com/user-attachments/assets/adf1dee6-b684-46ce-9da0-0108fcd7b7ae" />

---

#### Question 6: Peak Hour per Hari

Menampilkan jam tersibuk di setiap hari dalam seminggu.

```sql
SELECT
    order_day_name AS day_name,
    order_hour_of_day AS peak_hour,
    COUNT(DISTINCT order_id) AS total_orders
FROM orders_db.orders
GROUP BY order_dow, order_day_name, order_hour_of_day
ORDER BY order_dow, total_orders DESC
LIMIT 7;
```

**Visualisasi:** Table atau Bar Chart dengan grouping per hari

> <img width="578" height="233" alt="image" src="https://github.com/user-attachments/assets/f67e9e6d-1841-4d49-93a4-c9b8dbf334ee" />

---

#### Question 7: Rata-rata posisi cart per departemen

Mengukur seberapa "terencana" pembelian per department berdasarkan rata-rata urutan produk yang ditambahkan ke keranjang. Department dengan posisi rendah berarti produknya selalu ditambahkan pertama kali (sudah direncanakan), sedangkan posisi tinggi berarti lebih bersifat impulsif.

```sql
SELECT
    department,
    COUNT(*) AS total_items,
    ROUND(AVG(add_to_cart_order), 2) AS avg_cart_position
FROM orders_db.order_products
GROUP BY department
ORDER BY avg_cart_position ASC;
```

**Visualisasi:** Bar Chart dengan Sumbu Y: `department` dan Sumbu X: `avg_cart_position`

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/cc913992-b968-4143-a196-24b64d90b95a" />


---

### Question 8: Produk yang Paling Sering Ditambah Pertama ke Cart

Menampilkan produk yang paling konsisten ditambahkan sebagai item pertama dalam sesi belanja (add_to_cart_order = 1). Produk ini merupakan "anchor product" (titik awal yang memicu sesi belanja)

```sql
SELECT
    product_name,
    department,
    COUNT(*) AS times_added_first
FROM orders_db.order_products
WHERE add_to_cart_order = 1
GROUP BY product_name, department
ORDER BY times_added_first DESC
LIMIT 10;
```

**Visualisasi:** Bar Chart dengan Sumbu X: `product_name` dan Sumbu Y: `times_added_first`

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/be49eebf-9e82-4963-906b-484a6c2e804c" />

---

### Question 9: Rata-Rata Basket Size per Hari dalam Seminggu

Menghitung rata-rata jumlah item yang dibeli per order berdasarkan hari pemesanan. Memberikan gambaran apakah ada hari tertentu di mana pelanggan cenderung berbelanja lebih banyak item sekaligus.

```sql
SELECT
    o.order_dow,
    o.order_day_name AS day_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(op.product_id) AS total_items,
    ROUND(COUNT(op.product_id) /
          COUNT(DISTINCT o.order_id), 2) AS avg_basket_size
FROM orders_db.orders o
JOIN orders_db.order_products op ON o.order_id = op.order_id
GROUP BY o.order_dow, o.order_day_name
ORDER BY o.order_dow;
```

**Visualisasi:** Bar Chart dengan Sumbu X: `product_name` dan Sumbu Y: `times_added_first`

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/581eb0e8-08d1-4f55-a763-b685625d1f8e" />


---

### Question 10: Rata-Rata Basket Size per Time Category

Membandingkan rata-rata jumlah item dalam satu order berdasarkan kategori waktu belanja (pagi, siang, sore, malam). Menunjukkan apakah waktu belanja memengaruhi banyaknya item yang dibeli.

```sql
SELECT
    o.order_time_category AS time_category,
    ROUND(COUNT(op.product_id) /
          COUNT(DISTINCT o.order_id), 2) AS avg_basket_size
FROM orders_db.orders o
JOIN orders_db.order_products op ON o.order_id = op.order_id
GROUP BY o.order_time_category
ORDER BY avg_basket_size DESC;
```

**Visualisasi:** Line Chart dengan Sumbu X: `time_category` dan Sumbu Y: `avg_basket_size`

> <img height="250" alt="image" src="https://github.com/user-attachments/assets/d9155acb-17b1-475a-8b32-63ad54b93365" />


---

### Question 11: First Order vs Repeat Order: Basket Size & Reorder Rate

Membandingkan karakteristik belanja antara order pertama kali pelanggan dengan order lanjutan. Memberikan gambaran apakah pelanggan baru berbelanja lebih sedikit atau lebih banyak dibanding pelanggan yang sudah berulang

```sql
SELECT
    CASE WHEN o.is_first_order = 1
        THEN 'First Order'
        ELSE 'Repeat Order'
    END AS order_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(COUNT(op.product_id) /
          COUNT(DISTINCT o.order_id), 2) AS avg_basket_size,
    ROUND(SUM(op.is_reordered) * 100.0 /
          COUNT(op.product_id), 2) AS reorder_rate_pct
FROM orders_db.orders o
JOIN orders_db.order_products op ON o.order_id = op.order_id
GROUP BY o.is_first_order;
```

**Visualisasi:** Row Chart dengan Sumbu X: `Number` dan Sumbu Y: `avg_basket_size` & `reorder_rate_pct`

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/85554148-b539-4bc5-8bb7-1e5f1fbea5aa" />


---

### Question 12: Funnel Loyalitas Pelanggan

Menampilkan tahapan loyalitas pelanggan mulai dari semua order, lalu disaring ke repeat order, hingga yang benar-benar rutin membeli produk yang sama.

```sql
SELECT funnel_step, total
FROM (
    SELECT 1 AS sort_order, 'Total Orders' AS funnel_step,
        COUNT(DISTINCT o.order_id) AS total
    FROM orders_db.orders o

    UNION ALL

    SELECT 2, 'Repeat Orders',
        COUNT(DISTINCT o.order_id)
    FROM orders_db.orders o
    WHERE o.is_first_order = 0

    UNION ALL

    SELECT 3, 'Repeat + Reordered Item',
        COUNT(DISTINCT o.order_id)
    FROM orders_db.orders o
    JOIN orders_db.order_products op ON o.order_id = op.order_id
    WHERE o.is_first_order = 0
      AND op.is_reordered = 1

    UNION ALL

    SELECT 4, 'High Loyalty (reorder >50%)',
        COUNT(DISTINCT sub.order_id)
    FROM (
        SELECT op.order_id,
            ROUND(SUM(op.is_reordered) * 100.0 / COUNT(*), 0) AS rr
        FROM orders_db.order_products op
        JOIN orders_db.orders o ON op.order_id = o.order_id
        WHERE o.is_first_order = 0
        GROUP BY op.order_id
        HAVING rr > 50
    ) sub
) result
ORDER BY sort_order;
```

**Visualisasi:** Funnel Chart dengan Dimension: `funnel_step`, Measure: `total`, urutkan berdasarkan `sort_order`

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/ec4a19ef-c053-4688-8693-fdf6bac17a56" />

---

### Question 13: Distribusi interval belanja

seberapa sering pelanggan kembali secara distribusi.

```sql
SELECT
    CASE
        WHEN days_since_prior_order = 0    THEN 'First order'
        WHEN days_since_prior_order <= 7   THEN '1-7 hari'
        WHEN days_since_prior_order <= 14  THEN '8-14 hari'
        WHEN days_since_prior_order <= 30  THEN '15-30 hari'
        ELSE '30+ hari'
    END AS interval_bucket,
    COUNT(*) AS total_orders
FROM orders_db.orders
GROUP BY interval_bucket
ORDER BY MIN(days_since_prior_order);
```

**Visualisasi:** Pie chart

> <img height="280" alt="image" src="https://github.com/user-attachments/assets/d2717ddf-bb78-4764-85fa-2b44953ec620" />

---

### Question 14: Department mana yang paling banyak dibeli saat first order

Melihat apa yang dibeli pelanggan baru untuk memahami entry point (produk/department apa yang pertama kali menarik pelanggan masuk berbelanja).

```sql
SELECT
    op.department,
    COUNT(*) AS total_items
FROM orders_db.orders o
JOIN orders_db.order_products op ON o.order_id = op.order_id
WHERE o.is_first_order = 1
GROUP BY op.department
ORDER BY total_items DESC;
```
**Visualisasi:** Pie Chart dengan proporsi setiap department dari total 81 item yang dibeli pada first order.

> <img height="280" alt="image" src="https://github.com/user-attachments/assets/1d57e5db-0d0b-40ce-9e99-b3c5eaa17b42" />

---

### Question 15: Profil Setiap Department, Volume vs Loyalitas

Menampilkan profil lengkap setiap department dalam satu tabel, mencakup total item terjual, reorder rate, jumlah order yang mengandung department tersebut, dan jumlah produk unik. Memudahkan perbandingan antar department secara menyeluruh untuk mengidentifikasi mana yang tinggi volume sekaligus loyal, dan mana yang hanya ramai tapi tidak loyal.

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS dept_rank,
    department,
    COUNT(*) AS total_items,
    ROUND(SUM(is_reordered) * 100.0 / COUNT(*), 1) AS reorder_rate_pct,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT product_id) AS unique_products
FROM orders_db.order_products
GROUP BY department
ORDER BY total_items DESC;
```

**Visualisasi:** Tabel

> <img height="300" alt="image" src="https://github.com/user-attachments/assets/9f019ea6-d0d0-4a2c-b4f0-43abf635a553" />


## Langkah 4 — Membangun Dashboard di Metabase

### 4.1 Membuat Dashboard

1. Di Metabase, klik **New → Dashboard**
2. Beri nama dashboard, misalnya: **"Orders Analytics Dashboard — Kelompok 12"**
3. Klik **Add a saved question**, lalu tambahkan semua 6 question yang sudah dibuat di Langkah 3

### 4.2 Menyusun Tata Letak Dashboard

Atur posisi dan ukuran setiap visualisasi dengan cara drag-and-drop agar dashboard terlihat informatif dan rapi. Berikut susunan yang direkomendasikan:

4. Setelah selesai menyusun, klik **Save**

> <img width="290" height="470" alt="image" src="https://github.com/user-attachments/assets/21c2374f-48dc-46c7-9846-24698131a611" />
> <img width="435" height="451" alt="image" src="https://github.com/user-attachments/assets/74f9ca22-5dd0-4756-8df6-c60207cea05f" />

---

## Kendala & Solusi

Selama pengerjaan project ini, terdapat kendala utama yang ditemui pada bagian **Load to ClickHouse**. Berikut dokumentasinya:

### Kendala: Task `load_to_clickhouse` Gagal / Data Tidak Masuk

Pada awalnya, task `load_to_clickhouse` selalu gagal dengan berbagai error, antara lain:

1. **`Connection refused` ke ClickHouse** — Airflow mencoba konek ke ClickHouse sebelum container-nya selesai startup.
2. **`Code: 16. DB::Exception: No such column`** — Nama kolom yang di-INSERT tidak cocok persis dengan definisi tabel (urutan atau typo nama kolom).
3. **`TypeError: can't convert ... to datetime`** — Kolom `ingested_at` yang bertipe `DateTime` di ClickHouse tidak menerima string, harus berupa objek `datetime` Python.
4. **Data terduplikat** — Karena pipeline dijadwalkan setiap 10 menit dan file JSON tidak langsung dihapus saat terjadi error, data bisa ter-insert lebih dari sekali.

### Solusi yang Diterapkan

**1. Pencocokan nama kolom secara eksplisit**

Pada perintah `INSERT`, nama kolom disebutkan secara eksplisit satu per satu — tidak menggunakan wildcard. Urutan tuple data yang di-insert juga disesuaikan persis dengan urutan kolom yang dideklarasikan.

```python
client.execute(
    f"INSERT INTO {CLICKHOUSE_DB}.orders "
    f"(order_id, user_id, order_number, order_dow, order_hour_of_day, "
    f"days_since_prior_order, eval_set, total_products, order_day_name, "
    f"is_first_order, order_time_category, ingested_at) VALUES",
    order_rows
)
```

**2. Passing objek `datetime` langsung**

Kolom `ingested_at` diisi menggunakan `datetime.now()` yang di-assign sekali di awal fungsi dan diteruskan ke setiap row, bukan sebagai string.

```python
now = datetime.now()
# ...
order_rows.append((..., now))  # bukan str(now)
```

---

## Anggota Kelompok

| Nama | NRP | Kontribusi | Persentase |
|---|---|---|---|
| Safa Mashita | 5025241022 | Merancang dan mengimplementasikan Apache Airflow DAG (`orders_pipeline.py`), mengembangkan logika fetch & preprocessing data di `fetch_orders.py`, serta menyusun DDL schema ClickHouse dan memastikan pipeline berjalan end-to-end | 50% |
| Devina Balqis Aurora | 5025241034 | Mengimplementasikan `load_to_clickhouse.py` untuk proses ETL ke ClickHouse, membuat seluruh query analitik di Metabase, membangun dan menyusun layout Dashboard Metabase, serta mendokumentasikan README | 50% |

> **Kelompok 12 — MCI2026**
