Berikut adalah file `README.md` yang sudah disesuaikan dengan struktur folder asli kamu (di mana file script berada langsung di bawah folder `dags/`) dan mencakup semua poin teknis hasil debugging kita.

Silakan copy seluruh teks di bawah ini:

---

```markdown
# 🛒 Orders Data Pipeline — MCI 2026 Task 2

> **MCI 2026 · Pelatihan Big Data Lab** > Pipeline Orchestration & Data Visualization menggunakan Apache Airflow, ClickHouse, dan Metabase.

---

## 📖 Deskripsi Proyek

Proyek ini membangun sebuah **end-to-end data pipeline** yang mengambil data transaksi (*orders*) dari REST API, menyimpannya ke dalam data lake lokal, melakukan proses ingest ke data warehouse **ClickHouse**, dan memvisualisasikannya melalui dashboard interaktif di **Metabase**. Seluruh alur kerja ini dikelola secara otomatis oleh **Apache Airflow**.

---

## 🏗️ Arsitektur Sistem

Pipeline dirancang secara **Idempotent**, artinya task dapat dijalankan berulang kali tanpa menyebabkan duplikasi data karena adanya mekanisme *Truncate-then-Insert*.

```text
┌─────────────────────────────────────────────────────────┐
│                   Apache Airflow                        │
│                  (Orchestrator)                         │
│                                                         │
│   ┌─────────────────┐      ┌──────────────────────┐     │
│   │  Task 1         │      │  Task 2              │     │
│   │  fetch_orders   │ ───► │  load_to_clickhouse  │     │
│   │  (REST API)     │      │  (Insert ke DW)      │     │
│   └─────────────────┘      └──────────────────────┘     │
└─────────────────────────────────────────────────────────┘
         │                            │
         ▼                            ▼
  ┌─────────────┐            ┌──────────────────┐
  │  Data Lake  │            │    ClickHouse    │
  │ (JSON file) │            │  orders_db.orders│
  └─────────────┘            └──────────────────┘
                                      │
                                      ▼
                             ┌──────────────────┐
                             │    Metabase      │
                             │   (Dashboard)    │
                             └──────────────────┘

```

---

## 🛠️ Tech Stack

| Komponen | Teknologi | Keterangan |
| --- | --- | --- |
| **Orchestration** | Apache Airflow 2.9.0 | Manajemen workflow & penjadwalan |
| **Data Warehouse** | ClickHouse 23.8 | OLAP Database untuk analisis data besar |
| **Data Visualization** | Metabase | Dashboard BI dan visualisasi SQL |
| **Infrastructure** | Docker & Docker Compose | Containerization seluruh layanan |
| **Language** | Python 3.11 | Library: `clickhouse-driver`, `requests` |

---

## 📂 Struktur Proyek

Sesuai dengan repositori ini, berikut adalah struktur file utamanya:

```text
MCI2026_Task2_Kelompok12/
├── dags/
│   ├── fetch_orders.py          # Script untuk mengambil data dari API
│   ├── load_to_clickhouse.py    # Script ETL (Parsing JSON & Load ke ClickHouse)
│   └── orders_pipeline.py       # Definisi DAG Airflow
├── data_lake/                   # Penyimpanan file JSON (Staging area)
├── metabase_plugins/            # Driver database untuk Metabase
├── sql/
│   ├── ddl_orders.sql           # Skema tabel ClickHouse
│   └── queries_metabase.sql     # Kumpulan query untuk dashboard
├── docker-compose.yml           # Konfigurasi container Docker
├── Dockerfile                   # Custom image untuk dependencies
└── README.md

```

---

## 🚀 Panduan Instalasi & Eksekusi

### 1. Persiapan Container

Jalankan perintah berikut secara berurutan di terminal:

```powershell
# Build image dan inisialisasi Airflow
docker-compose build
docker-compose up airflow-init

# Jalankan semua service secara background
docker-compose up -d

```

### 2. Menjalankan Pipeline di Airflow

1. Buka `http://localhost:8080` (User: `admin` | Pass: `admin`).
2. Aktifkan DAG **`orders_pipeline`**.
3. Klik tombol **Trigger DAG**.
4. **Catatan Debugging:** Task `load_to_clickhouse` telah diperkuat untuk menangani format JSON baik berupa *List* maupun *Dictionary* secara otomatis untuk menghindari `AttributeError`.

### 3. Setup Metabase

1. Buka `http://localhost:3000`.
2. Tambahkan Database **ClickHouse**.
3. Konfigurasi Koneksi:
* **Host:** `clickhouse-server`
* **Port:** `8123`
* **Database:** `orders_db`
* **Username:** `admin`
* **Password:** `rahasia`



---

## 📊 Business Insights (Metabase)

Beberapa metrik utama yang ditampilkan dalam dashboard:

* **Total Repeat Orders:** Mengetahui loyalitas pelanggan berdasarkan jumlah pesanan berulang.
* **Peak Ordering Hours:** Menentukan jam operasional tersibuk untuk optimasi *resource*.
* **Top 10 Selling Products:** Analisis produk yang paling banyak diminati pasar.
* **Order Distribution (DOW):** Tren belanja pelanggan berdasarkan hari dalam seminggu.

---

## 🔧 Troubleshooting (Hasil Debugging)

* **Format JSON:** Jika API mengembalikan List `[...]`, script akan otomatis melakukan mapping tanpa error `.get()`.
* **Empty Data:** Jika `SELECT count()` di ClickHouse menghasilkan 0, pastikan file di `data_lake/` tidak kosong dan task `load_to_clickhouse` tidak terlewati.
* **Metabase Driver:** Driver ClickHouse telah disediakan di folder `metabase_plugins/` untuk memastikan koneksi lancar.

---

## 👥 Anggota Kelompok 12

* [Nama Anda] - [NRP]
* [Nama Rekan] - [NRP]

---

*Proyek ini diselesaikan sebagai bagian dari Task 2 MCI 2026.*

```

```
