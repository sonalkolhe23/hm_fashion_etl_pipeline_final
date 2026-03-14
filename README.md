# H&M Fashion Retail Data Pipeline
### Masters Final Project — PostgreSQL On-Premise ETL

---
Dataset Notice
The datasets are not included due to GitHub’s 100 MB file limit. Please download the required files from Kaggle and place them in the folders below before running the pipeline.

## Overview

This pipeline merges two Kaggle data sources into a PostgreSQL data warehouse
and answers 6 retail business objectives using SQL.

| Source | Type | Files |
|---|---|---|
| H&M product catalog + customer profiles | Object Storage | `articles.csv`, `customers.csv` |
| H&M purchase history | On-Premise file | `transactions_train.csv` |

---

## Kaggle Dataset

URL: https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data

Download these 3 files and place them as shown:

| File | Place in |
|---|---|
| `articles.csv` | `data/object_storage/` |
| `customers.csv` | `data/object_storage/` |
| `transactions_train.csv` | `data/on_premise/` |

---

## Setup

### 1. Install PostgreSQL
Download from: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
Keep default port 5432 and set a password during installation.

### 2. Create the database
```bash
psql -U postgres -c "CREATE DATABASE fashion_dw;"
```

### 3. Install Python packages
```bash
pip install -r requirements.txt
```

### 4. Set your password
Open `pipeline/etl_pipeline.py` and update the password in the db_config block at the top of the file.

---

## Running the Pipeline

```bash
# Quick test (50,000 rows)
python pipeline/etl_pipeline.py --sample

# Full run (31 million rows)
python pipeline/etl_pipeline.py
```

---

## Testing with New Data

```bash
python pipeline/add_new_data.py --sample
```

This adds new rows to both source files and re-runs the pipeline automatically.
It demonstrates that the pipeline picks up new data from both sources and updates all outputs.

---

## Business Objectives

| # | Question | Output |
|---|---|---|
| 1 | Avg sale price per category per year | `obj1_avg_price_per_category.csv` |
| 2 | Top 20 best-selling articles | `obj2_top20_articles.csv` |
| 3 | Monthly revenue trend | `obj3_monthly_trend.csv` |
| 4 | Customer segments by age + club status | `obj4_customer_segments.csv` |
| 5 | Online vs In-Store by department | `obj5_online_vs_store.csv` |
| 6 | Customer loyalty and repeat purchases | `obj6_loyalty.csv` |

---

## Folder Structure

```
hm_pipeline/
├── data/
│   ├── object_storage/             <- SOURCE 1: articles.csv, customers.csv
│   └── on_premise/                 <- SOURCE 2: transactions_train.csv
├── pipeline/
│   ├── etl_pipeline.py             <- Main pipeline script
│   └── add_new_data.py             <- Testing script
├── sql/
│   ├── 01_schema.sql               <- Creates tables and views
│   └── 02_business_objectives.sql  <- SQL queries for pgAdmin
├── outputs/                        <- Auto-generated CSVs
├── docs/
│   ├── screenshots/                <- Project screenshots
│── ai_prompts.txt              <- AI prompts used
├── requirements.txt
└── README.md
```

---

## Notes

- Data files are not included in this repository due to file size
- Download from the Kaggle link above and place in the correct folders
- Output CSV files are generated automatically when the pipeline runs
