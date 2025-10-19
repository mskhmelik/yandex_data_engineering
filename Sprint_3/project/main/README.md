 # 🧱 ETL & Data mart project

This project automates the full ETL pipeline for a fictional retail dataset provided by **Yandex Practicum**.  
The pipeline extracts data from the provided API, stages it, and builds several marts for analytics.

---

## 📦 Project Structure

| Folder / File | Description |
|----------------|-------------|
| `src/sales_mart_dag.py` | Main Airflow DAG — orchestrates extraction from API and loading into marts |
| `migrations/mart.f_sales.sql` | Builds transactional fact table of all sales and refunds |
| `migrations/mart.f_customer_retention.sql` | Builds weekly customer retention fact |
| `migrations/mart.d_city.sql` | Builds city dimension |
| `migrations/mart.d_customer.sql` | Builds customer dimension |
| `migrations/mart.d_item.sql` | Builds item/product dimension |
| `requirements.txt` | Dependencies (Airflow, requests, pandas, etc.) |
| `README.md` | This file |

---

## 🚀 How It Works

### 1. Extraction (API)
The DAG connects to the Practicum API using the following endpoints:

| Method | Endpoint | Description |
|---------|-----------|-------------|
| `POST /generate_report` | Requests full data report |
| `GET /get_report` | Polls for report completion and retrieves report ID |
| `GET /get_increment` | Fetches daily increment files (customer, order, activity logs) |

Each call includes authentication headers (`X-Nickname`, `X-Cohort`, `X-API-KEY`, etc.) and stores files from Yandex S3 into staging.

---

### 2. Staging
Files are read via Pandas, cleaned, and loaded into Postgres schemas:
- `staging.user_order_log`
- `staging.customer_research`
- `staging.user_activity_log`
- `staging.price_log`

Each incremental run adds or updates rows using a natural key (`uniq_id`).

---

### 3. Marts
Marts are refreshed daily via SQL scripts stored in `migrations/`:

#### `mart.f_sales`
Transactional fact table:
- Stores each order (and refund) with signed revenue and quantity.
- Upsert logic ensures **idempotency** (`ON CONFLICT (uniq_id)`).
- Refunds are stored as negative values.

#### `mart.f_customer_retention`
Weekly customer retention metrics:
- Calculates number of **new**, **returning**, and **refunded** customers per `item_id`.
- Computes revenue for new and returning customers.
- Deletes target week before reload to avoid duplicates.

#### `mart.d_city`, `mart.d_customer`, `mart.d_item`
Simple slowly-changing dimensions updated by distinct values from staging tables.
Each uses:
```sql
ON CONFLICT (id) DO UPDATE
SET column = EXCLUDED.column;
```
so reruns are safe.