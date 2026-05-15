# 📦 MCI2026 Task 2 — Pipeline Orchestration & Data Visualization
**Kelompok 12 | Modul 2 & 3**

---

## 📋 Daftar Isi

1. [Gambaran Umum](#-gambaran-umum)
2. [Arsitektur Pipeline](#-arsitektur-pipeline)
3. [Struktur Repository](#-struktur-repository)
4. [Prasyarat & Instalasi](#-prasyarat--instalasi)
5. [Langkah 1 — Apache Airflow DAG](#-langkah-1--apache-airflow-dag)
6. [Langkah 2 — Manajemen Data di ClickHouse](#-langkah-2--manajemen-data-di-clickhouse)
7. [Langkah 3 — Visualisasi & Questions di Metabase](#-langkah-3--visualisasi--questions-di-metabase)
8. [Langkah 4 — Membangun Dashboard di Metabase](#-langkah-4--membangun-dashboard-di-metabase)
9. [Kendala & Solusi](#-kendala--solusi)

---

## 🔍 Gambaran Umum

Proyek ini merupakan implementasi **Pipeline Orchestration & Data Visualization** menggunakan stack berikut:

| Komponen | Teknologi |
|---|---|
| Orkestrasi Pipeline | Apache Airflow 2.9.0 |
| Columnar Database | ClickHouse 23.8 |
| Visualisasi & Dashboard | Metabase |
| Sumber Data | REST API `http://96.9.212.102:8000/orders` |
| Containerization | Docker & Docker Compose |

Dataset yang digunakan berformat Instacart-style orders, berisi data transaksi belanja online beserta detail produk yang dipesan oleh masing-masing pengguna.

---

## 🏗️ Arsitektur Pipeline

```
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                    │
│                                                         │
│   ┌─────────────┐           ┌──────────────────────┐   │
│   │ fetch_orders│ ────────► │ load_to_clickhouse   │   │
│   │  (Task 1)   │           │      (Task 2)        │   │
│   └─────────────┘           └──────────────────────┘   │
│         │                            │                  │
│    Fetch API &                  Read JSON &             │
│    Preprocessing               Insert ke ClickHouse     │
└─────────────────────────────────────────────────────────┘
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

## 📂 Struktur Repository

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

## ⚙️ Prasyarat & Instalasi

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

> 📸 **[Screenshot: Output `docker compose ps` menampilkan semua service running]**

**4. Akses layanan**

| Layanan | URL | Kredensial |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| ClickHouse HTTP | http://localhost:8123 | `admin` / `rahasia` |
| Metabase | http://localhost:3000 | (setup awal saat pertama buka) |

---

## 🌀 Langkah 1 — Apache Airflow DAG

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

> 📸 **[Screenshot: Airflow UI — task `fetch_orders` berstatus Success]**

### 1.3 Task 2: `load_to_clickhouse`

File: `dags/load_to_clickhouse.py`

Task ini bertanggung jawab untuk:
- Membaca semua file JSON dari folder `data_lake`
- Memisahkan data menjadi dua list: `order_rows` dan `product_rows`
- Melakukan **TRUNCATE** lalu **INSERT** ke tabel ClickHouse
- Menghapus file JSON setelah proses berhasil

> 📸 **[Screenshot: Airflow UI — task `load_to_clickhouse` berstatus Success]**

> 📸 **[Screenshot: Airflow UI — tampilan graph/tree view DAG `orders_pipeline` dengan kedua task hijau]**

---

## 🗄️ Langkah 2 — Manajemen Data di ClickHouse

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

**Tabel `order_products`** — menyimpan detail produk per transaksi (hasil flatten dari nested array):
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

Setelah pipeline berjalan, lakukan validasi dengan query berikut melalui ClickHouse HTTP Interface (`http://localhost:8123`):

```sql
-- Cek jumlah baris
SELECT COUNT(*) FROM orders_db.orders;
SELECT COUNT(*) FROM orders_db.order_products;

-- Preview data
SELECT * FROM orders_db.orders LIMIT 5;
SELECT * FROM orders_db.order_products LIMIT 5;
```

> 📸 **[Screenshot: Hasil query COUNT(*) di ClickHouse menampilkan jumlah baris yang sudah masuk]**

> 📸 **[Screenshot: Preview 5 baris pertama tabel `orders` dan `order_products`]**

---

## 📊 Langkah 3 — Visualisasi & Questions di Metabase

### 3.1 Koneksi Metabase ke ClickHouse

1. Buka Metabase di `http://localhost:3000`
2. Selesaikan setup awal (buat akun admin)
3. Masuk ke **Settings → Admin → Databases → Add a Database**
4. Pilih **ClickHouse** sebagai database type
5. Isi konfigurasi:
   - **Host:** `clickhouse-server`
   - **Port:** `8123`
   - **Database:** `orders_db`
   - **Username:** `admin`
   - **Password:** `rahasia`
6. Klik **Save**

> 📸 **[Screenshot: Halaman konfigurasi koneksi database ClickHouse di Metabase]**

> 📸 **[Screenshot: Konfirmasi koneksi berhasil / database `orders_db` muncul di daftar]**

### 3.2 Membuat Questions (Queries)

Setiap question dibuat melalui menu **New → Question → Native Query** di Metabase.

---

#### Question 1 — Total Order per Hari

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

**Visualisasi:** Bar Chart — sumbu X: `day_name`, sumbu Y: `total_orders`

> 📸 **[Screenshot: Hasil query Q1 di Metabase dengan visualisasi bar chart]**

---

#### Question 2 — Top 10 Produk Terlaris

Menampilkan 10 produk yang paling banyak dipesan beserta kategori departemennya.

```sql
SELECT product_name, department, COUNT(*) AS total_ordered
FROM orders_db.order_products
GROUP BY product_name, department
ORDER BY total_ordered DESC
LIMIT 10;
```

**Visualisasi:** Bar Chart (horizontal) — sumbu Y: `product_name`, sumbu X: `total_ordered`

> 📸 **[Screenshot: Hasil query Q2 di Metabase dengan visualisasi bar chart horizontal]**

---

#### Question 3 — Top Department

Menampilkan kategori departemen yang paling banyak menghasilkan penjualan produk.

```sql
SELECT department, COUNT(*) AS total_products_sold
FROM orders_db.order_products
GROUP BY department
ORDER BY total_products_sold DESC;
```

**Visualisasi:** Pie Chart atau Bar Chart — menunjukkan proporsi penjualan per departemen

> 📸 **[Screenshot: Hasil query Q3 di Metabase dengan visualisasi pie/bar chart]**

---

#### Question 4 — Reorder Rate per Department

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

**Visualisasi:** Bar Chart — sumbu X: `product_category`, sumbu Y: `reorder_rate_pct`

> 📸 **[Screenshot: Hasil query Q4 di Metabase menampilkan reorder rate per departemen]**

---

#### Question 5 — First Order vs Repeat Order

Membandingkan jumlah pelanggan baru (first order) dengan pelanggan yang sudah pernah berbelanja sebelumnya (repeat order).

```sql
SELECT
    CASE WHEN is_first_order = 1 THEN 'First Order' ELSE 'Repeat Order' END AS order_type,
    COUNT(*) AS total
FROM orders_db.orders
GROUP BY is_first_order;
```

**Visualisasi:** Pie Chart — proporsi antara `First Order` dan `Repeat Order`

> 📸 **[Screenshot: Hasil query Q5 di Metabase dengan visualisasi pie chart]**

---

#### Question 6 — Peak Hour per Hari

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

> 📸 **[Screenshot: Hasil query Q6 di Metabase menampilkan peak hour per hari]**

---

## 🖥️ Langkah 4 — Membangun Dashboard di Metabase

### 4.1 Membuat Dashboard

1. Di Metabase, klik **New → Dashboard**
2. Beri nama dashboard, misalnya: **"Orders Analytics Dashboard — Kelompok 12"**
3. Klik **Add a saved question**, lalu tambahkan semua 6 question yang sudah dibuat di Langkah 3

### 4.2 Menyusun Tata Letak Dashboard

Atur posisi dan ukuran setiap visualisasi dengan cara drag-and-drop agar dashboard terlihat informatif dan rapi. Berikut susunan yang direkomendasikan:

| Baris | Card | Ukuran |
|---|---|---|
| 1 | Q5: First vs Repeat Order (Pie) | Kecil |
| 1 | Q1: Total Order per Hari (Bar) | Besar |
| 2 | Q3: Top Department (Bar/Pie) | Sedang |
| 2 | Q4: Reorder Rate per Department (Bar) | Sedang |
| 3 | Q2: Top 10 Produk Terlaris (Bar Horizontal) | Full width |
| 4 | Q6: Peak Hour per Hari (Table/Bar) | Full width |

4. Setelah selesai menyusun, klik **Save**

> 📸 **[Screenshot: Tampilan lengkap Dashboard Metabase dengan semua 6 visualisasi tersusun rapi]**

> 📸 **[Screenshot: Detail salah satu card dashboard — misalnya Q2 Top 10 Produk Terlaris]**

---

## 🔧 Kendala & Solusi

Selama pengerjaan project ini, terdapat kendala utama yang ditemui pada bagian **Load to ClickHouse**. Berikut dokumentasinya:

### Kendala: Task `load_to_clickhouse` Gagal / Data Tidak Masuk

Pada awalnya, task `load_to_clickhouse` selalu gagal dengan berbagai error, antara lain:

1. **`Connection refused` ke ClickHouse** — Airflow mencoba konek ke ClickHouse sebelum container-nya selesai startup.
2. **`Code: 16. DB::Exception: No such column`** — Nama kolom yang di-INSERT tidak cocok persis dengan definisi tabel (urutan atau typo nama kolom).
3. **`TypeError: can't convert ... to datetime`** — Kolom `ingested_at` yang bertipe `DateTime` di ClickHouse tidak menerima string, harus berupa objek `datetime` Python.
4. **Data terduplikat** — Karena pipeline dijadwalkan setiap 10 menit dan file JSON tidak langsung dihapus saat terjadi error, data bisa ter-insert lebih dari sekali.

### Solusi yang Diterapkan

**1. Dependency antar container di `docker-compose.yml`**

Ditambahkan `depends_on` pada service Airflow agar menunggu ClickHouse siap sebelum mulai. Selain itu, pada kode Python, koneksi ke ClickHouse dibungkus dengan `try/except` sehingga error koneksi tercatat di log dan task langsung gagal dengan pesan yang jelas.

**2. Pencocokan nama kolom secara eksplisit**

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

**3. Passing objek `datetime` langsung**

Kolom `ingested_at` diisi menggunakan `datetime.now()` yang di-assign sekali di awal fungsi dan diteruskan ke setiap row, bukan sebagai string.

```python
now = datetime.now()
# ...
order_rows.append((..., now))  # bukan str(now)
```

**4. Pola TRUNCATE sebelum INSERT**

Untuk menghindari duplikasi, diterapkan pola **TRUNCATE → INSERT** setiap kali pipeline berjalan. Ini memastikan tabel selalu berisi data segar dari run terakhir.

```python
client.execute(f"TRUNCATE TABLE {CLICKHOUSE_DB}.orders")
client.execute(f"TRUNCATE TABLE {CLICKHOUSE_DB}.order_products")
# baru INSERT...
```

**5. Hapus file JSON setelah berhasil**

File JSON di `data_lake` hanya dihapus setelah proses INSERT berhasil sepenuhnya, sehingga jika terjadi error di tengah proses, file tetap ada dan bisa diproses ulang di run berikutnya.

---

## 👥 Anggota Kelompok

| Nama | NRP | Kontribusi | Persentase |
|---|---|---|---|
| Safa Mashita | 5025241022 | Membuat Apache Airflow DAG, preprocessing data, schema ClickHouse, integrasi pipeline, dan dokumentasi proyek | 50% |
| Devina Balqis Aurora | 5025241034 | Membuat proses ETL ke ClickHouse, query analitik Metabase, dashboard visualization, dan dokumentasi proyek | 50% |

> **Kelompok 12 — MCI2026**
