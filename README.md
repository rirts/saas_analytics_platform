# SaaS Analytics Platform (dbt + DuckDB)

This project is a local, fully free analytics stack for a B2B SaaS business.
It simulates a modern analytics pipeline using:

- HubSpot synthetic CRM data – companies, contacts, deals
- Stripe synthetic billing data – customers, invoices, subscriptions
- Product events – in-app usage events
- dbt + DuckDB – for modeling, testing, and exposing SaaS metrics

Everything runs locally on DuckDB with dbt, no cloud accounts and no paid services required.

---

## 1. Goals

- Demonstrate how to model a SaaS business with a modern analytics engineering stack.
- Use dbt to build a clean layered architecture:
  - Seeds → Staging → Intermediate → Marts
- Expose key SaaS metrics:
  - Pipeline and bookings
  - Subscription MRR / ARR
  - Basic product usage behaviour
- Include data tests to showcase data quality and contracts.

---

## 2. Tech stack

- dbt Core 1.10.x
- dbt-duckdb 1.10.x
- DuckDB (file-based warehouse)
- Python 3.11+ (recommended)

---

## 3. Project structure

This repository contains the warehouse layer only (dbt project in the repo root):

    saas_analytics_platform/
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/
    │   │   ├── events/
    │   │   │   └── stg_events__product_events.sql
    │   │   ├── hubspot/
    │   │   │   ├── stg_hubspot__companies.sql
    │   │   │   ├── stg_hubspot__contacts.sql
    │   │   │   └── stg_hubspot__deals.sql
    │   │   └── stripe/
    │   │       ├── stg_stripe__customers.sql
    │   │       ├── stg_stripe__invoices.sql
    │   │       └── stg_stripe__subscriptions.sql
    │   ├── intermediate/
    │   │   ├── billing/
    │   │   │   └── int_billing__subscriptions_enriched.sql
    │   │   └── crm/
    │   │       └── int_crm__deals_enriched.sql
    │   └── marts/
    │       └── crm/
    │           ├── mart_crm__bookings_monthly.sql
    │           └── mart_crm__pipeline_current.sql
    ├── seeds/
    │   ├── events/
    │   │   └── product_events_raw.csv
    │   ├── hubspot/
    │   │   ├── companies_raw_hubspot.csv
    │   │   ├── contacts_raw_hubspot.csv
    │   │   └── deals_raw_hubspot.csv
    │   └── stripe/
    │       ├── customers_raw_stripe.csv
    │       ├── invoices_raw_stripe.csv
    │       └── subscriptions_raw_stripe.csv
    ├── profiles_sample.yml
    └── README.md

---

## 4. How to run it locally

All commands below assume you are in the repo root (where dbt_project.yml is).

### 4.1. Create a virtual environment

```bash
python -m venv .venv
```

Activate it:

On Linux / macOS:
```bash
source .venv/bin/activate
```

On Windows (PowerShell):
```bash
.\.venv\Scripts\Activate.ps1
```

### 4.2. Install dependencies

Create a requirements.txt in the repo root with:

```yaml
dbt-duckdb==1.10.0
duckdb==1.1.0
```

Then install:

```bash
pip install -r requirements.txt
```

### 4.3. Configure profiles.yml

Create or edit ~/.dbt/profiles.yml
(on Windows: C:\Users\<your_user>\.dbt\profiles.yml) and add:

```yaml
saas_analytics_platform:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "analytics.duckdb"
      threads: 4
```

The profile name saas_analytics_platform must match the profile value in dbt_project.yml.

### 4.4. Check the connection

```bash
dbt debug
```

You should see: All checks passed!

### 4.5. Load seed data

```bash
dbt seed
```

This loads the CSV files into DuckDB as raw tables (HubSpot, Stripe, Events).

### 4.6. Build the models

Build everything:

```bash
dbt build
```

Or by layer:

```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt test
```
---

## 5. Data model overview

### 5.1. Staging layer

stg_events__product_events
Normalizes raw product events into a clean schema with:
- event_id, user_id, account_id
- event_type
- event_timestamp_utc

stg_hubspot__companies
Cleans company records from HubSpot.

stg_hubspot__contacts
Cleans contact records (one row per contact).

stg_hubspot__deals
Standardizes deals, keeping only fields needed for pipeline and bookings.

stg_stripe__customers, stg_stripe__invoices, stg_stripe__subscriptions
Normalize Stripe billing data, convert Unix timestamps to proper timestamps, and add synthetic MRR/ARR logic based on plan.

### 5.2. Intermediate layer

int_crm__deals_enriched
- Adds deal_status (open, won, lost, closed_other)
- booking_date
- sales_cycle_days
- won_amount_company_currency / lost_amount_company_currency

int_billing__subscriptions_enriched
- Joins subscriptions, customers, and aggregated invoices
- Subscription status flags (is_active, is_canceled, is_trialing)
- MRR and ARR
- total_invoiced_usd, total_paid_usd
- last_invoice_at, has_any_paid_invoice

### 5.3. Marts

mart_crm__bookings_monthly
- Monthly bookings by booking_month
- Broken down by pipeline, deal_stage, deal_status, etc.

mart_crm__pipeline_current
- Current open pipeline snapshot
- One row per deal with stage, status, and amounts.

These marts are meant to feed BI dashboards (Metabase, Power BI, etc.) with minimal extra logic.

---

## 6. How to query the data (optional)

You can open the DuckDB database directly from Python:

```python
import duckdb

con = duckdb.connect("analytics.duckdb")
con.execute("SELECT * FROM mart_crm__bookings_monthly LIMIT 10").df()
```

Or use any DuckDB client that can connect to a local .duckdb file.

---

## 7. Limitations and next steps

- All data is synthetic and small; the goal is clarity, not scale.
- Everything runs locally; there is no cloud warehouse or orchestration in this version.
- Possible extensions:
  - Add orchestration (Airflow, Prefect, Dagster) to schedule dbt.
  - Connect a BI tool (Metabase, Power BI, etc.) directly to the DuckDB file.
  - Add more mart models (retention, cohorts, LTV, churn).

---

## 8. License

You can adapt, extend, and reuse this project freely in your own portfolio or learning path.
If you fork it, consider keeping a link back to the original inspiration.
