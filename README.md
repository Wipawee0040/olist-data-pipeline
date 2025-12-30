# 🇧🇷 Olist E-Commerce Data Pipeline

An end-to-end data engineering project that processes the **Brazilian E-Commerce Public Dataset by Olist**. This pipeline transforms raw CSV data into a clean, analytical **Star Schema** ready for business intelligence, utilizing a modern data stack (Databricks, Spark, and dbt).

![Project Architecture](dbt-dag.png)
*(Place your dbt DAG or Lineage image here)*

## 🚀 Project Overview

The goal of this project is to build a robust data pipeline that handles raw data ingestion, cleans data anomalies (schema drift), and models it for analytics.

* **Source:** 9 raw CSV files (Customers, Orders, Payments, etc.)
* **Processing Engine:** Apache Spark (Databricks) for heavy lifting & ingestion.
* **Transformation:** dbt (Data Build Tool) for modular SQL transformation & testing.
* **Destination:** Star Schema (Fact & Dimension tables).

## 🛠 Tech Stack

* **Language:** Python (PySpark), SQL
* **Processing:** Databricks, Apache Spark
* **Transformation & Modeling:** dbt Core
* **Orchestration & Version Control:** Git, GitHub
* **IDE:** VS Code (with dbt Power User extension)

## 📂 Project Structure

This project adopts a **Hybrid Architecture**, separating raw file processing from warehouse modeling:

```bash
AE_dbt_transform/
├── notebooks/           # 🐍 BRONZE & SILVER Layer (Spark/Python)
│   ├── bronze_layer.py  # Ingest raw CSVs, handle schema drift/multiline
│   └── silver_layer_1.py# Deduplication and initial cleaning
├── models/              # 🏗️ GOLD Layer (dbt/SQL)
│   ├── staging/         # View materialization
│   └── mart/            # Fact & Dimension tables (Star Schema)
├── tests/               # Data Quality tests
└── dbt_project.yml

🏗️ Architecture & Implementation
1. Ingestion (Bronze Layer) - Apache Spark
Raw data contained complex issues like multiline records and column shifts that standard SQL could not handle.
- Solution: Used PySpark on Databricks to read, clean, and standardize the schema.
- Key Code: Located in /notebooks.

2. Transformation (Gold Layer) - dbt
Transformed data into a Star Schema optimized for BI tools.
- Fact Table: fact_orders (Centralized transactions, revenue, and order status).
- Dimension Tables: dim_customers, dim_products, dim_date, etc.
- Technique: Used CTEs (Common Table Expressions) for readable code and COALESCE to handle NULL values in financial calculations.

3. Data Quality & Testing
Data integrity is enforced using dbt tests. The pipeline will fail if critical business logic is violated.
- Tests Implemented:
- not_null & unique: On Primary Keys.
- Business Logic: "Total payment amount must be non-negative" (>= 0).
- Relationships: Referential integrity between Fact and Dimensions.

🔗 How to Run
Clone the repository:

Bash

git clone [https://github.com/YourUsername/olist-data-pipeline.git](https://github.com/YourUsername/olist-data-pipeline.git)
Setup Environment:

Bash

pip install dbt-databricks
Run Pipeline:

Bash

dbt deps
dbt run
dbt test

** Created by Wipawee Raksasat - 30/12/2025 **
