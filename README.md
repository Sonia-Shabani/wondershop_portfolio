# WonderShop Data Warehouse Portfolio

## Overview
This project demonstrates the design and implementation of a **PostgreSQL-based data warehouse** for a fictional retail company, *WonderShop*.  
It focuses on **real-world data engineering patterns**, including staging layers, incremental loads, Slowly Changing Dimensions (SCD), fact tables, and data quality checks.

The project is designed as a **portfolio project** to showcase practical data warehouse and analytics engineering skills.

---

## Architecture
The warehouse follows a classic **layered architecture**:
Source CSV Files
|
v
+----------------+
| STAGING | (append-only raw data)
+----------------+
|
v
+----------------+
| WAREHOUSE | (clean, modeled data)
+----------------+
|
v
Analytics / Reporting

### Layers
- **Staging**
  - Raw, append-only tables
  - No transformations except basic ingestion
  - Keeps full history of incoming data
- **Warehouse**
  - Cleaned and modeled tables
  - Dimensions (Type 1 SCD)
  - Incremental fact tables
  - Data quality enforcement

---

## Data Model

### Dimensions
- **dim_product**
  - Product attributes (category, season, prices, flags)
  - SCD Type 1 (latest state only)
- **dim_country**
  - Country and currency mapping
- **dim_date**
  - Calendar dimension (day, month, quarter, year)
- **map_order_status**
  - Mapping of raw multilingual order statuses to standardized values
- **fx_rates_monthly**
  - Monthly average FX rates to EUR

### Facts
- **sales_current**
  - Latest version of each order line
  - Incremental upserts using hash-based change detection
- **stock_snapshot**
  - Monthly stock snapshots by country and product

---

## Key Features

- **Incremental Loads**
  - Reprocesses current and previous month to handle late-arriving updates
- **Hash-Based Upserts**
  - Business key hash for idempotent loads
  - Change hash to avoid unnecessary updates
- **Slowly Changing Dimensions (Type 1)**
  - Product attributes always reflect the latest known state
- **Data Quality Checks**
  - Missing keys
  - Invalid numeric values
  - Unmapped statuses
  - Referential integrity between facts and dimensions
- **FX Rate Integration**
  - Monthly average exchange rates for multi-currency analysis

---

## Tech Stack
- **Database:** PostgreSQL
- **Languages:** SQL
- **Version Control:** Git & GitHub
- **Data Sources:** CSV files (simulated operational exports)

---

## Repository Structure
wondershop_portfolio/
│
├── sql/
│ ├── 01_create_schemas.sql
│ ├── 02_create_staging_tables.sql
│ ├── 04_load_staging_sales.sql
│ ├── 04_load_staging_product.sql
│ ├── 04_load_staging_stock.sql
│ ├── 05_create_warehouse_tables.sql
│ ├── 06_load_dimensions.sql
│ ├── 07_load_facts.sql
│ └── 08_data_quality_checks.sql
│
├── README.md
└── .gitignore
---

## How to Run (High Level)

1. Create schemas and staging tables  
   ```sql
   01_create_schemas.sql
   02_create_staging_tables.sql
   
2. Load raw data into staging
   04_load_staging_sales.sql
04_load_staging_product.sql
04_load_staging_stock.sql

3. Create warehouse tables
   05_create_warehouse_tables.sql

4. Load dimensions and facts
   06_load_dimensions.sql
07_load_facts.sql

5. Run data quality checks
 08_data_quality_checks.sql

  Data Quality Philosophy

Staging data is never modified

All validations happen in the warehouse layer

Quality checks are explicit, query-based, and reproducible

Any non-zero result in checks indicates data issues that should be reviewed 

Purpose of This Project

This project was built to:

Practice real-world data warehouse modeling

Demonstrate incremental ETL logic

Showcase clean SQL and professional Git workflows

Serve as a portfolio project for data engineering / analytics roles     
   
Sonia Shabani
GitHub: https://github.com/Sonia-Shabani   
