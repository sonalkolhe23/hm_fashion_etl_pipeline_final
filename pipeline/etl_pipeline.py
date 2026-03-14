import os
import sys
import argparse
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from pathlib import Path

# folder paths
BASE = Path(__file__).resolve().parent.parent
OBJECT_STORAGE = BASE / "data" / "object_storage"
ON_PREMISE = BASE / "data" / "on_premise"
SQL_FOLDER = BASE / "sql"
OUTPUT_FOLDER = BASE / "outputs"
OUTPUT_FOLDER.mkdir(exist_ok=True)

# database connection details

db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fashion_dw",
    "user": "postgres",
    "password": "Shubham@17"
}


def connect_db():
    try:
        conn = psycopg2.connect(**db_config)
        print(f"connected to database: {db_config['dbname']}")
        return conn
    except Exception as e:
        print(f"could not connect to database: {e}")
        print("make sure postgresql is running")
        sys.exit(1)


def setup_schema(conn):
    print("\nSetting up database schema...")
    sql_file = (SQL_FOLDER / "01_schema.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(sql_file)
    conn.commit()
    print("Schema created successfully")


def load_data(use_sample=False):
    row_limit = 50000 if use_sample else None

    # source 1 - object storage
    print("\nReading from Object Storage...")
    print("Loading articles.csv...")
    articles_df = pd.read_csv(OBJECT_STORAGE / "articles.csv", dtype=str)
    print(f"  articles loaded: {len(articles_df)}")

    print("Loading customers.csv...")
    customers_df = pd.read_csv(OBJECT_STORAGE / "customers.csv", dtype=str)
    print(f"  customers loaded: {len(customers_df)}")

    # source 2 - on premise file
    print("\nReading from On-Premise file...")
    print("Loading transactions_train.csv...")
    transactions_df = pd.read_csv(
        ON_PREMISE / "transactions_train.csv",
        dtype={"article_id": str, "customer_id": str},
        nrows=row_limit
    )
    print(f"  transactions loaded: {len(transactions_df)}")

    return articles_df, customers_df, transactions_df


def clean_articles(df):
    print("\nCleaning articles data...")
    df = df.copy()
    df.columns = [col.lower().strip() for col in df.columns]
    df["article_id"] = df["article_id"].astype(str).str.zfill(10)

    for col in df.select_dtypes("object").columns:
        df[col] = df[col].fillna("Unknown")

    number_cols = ["product_type_no", "graphical_appearance_no", "department_no",
                   "index_group_no", "section_no", "garment_group_no"]
    for col in number_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  articles after cleaning: {len(df)}")
    return df


def clean_customers(df):
    print("Cleaning customers data...")
    df = df.copy()
    df.columns = [col.lower().strip() for col in df.columns]
    df["age"] = pd.to_numeric(df.get("age"), errors="coerce")

    def get_age_group(age):
        if pd.isna(age):
            return "Unknown"
        elif age < 18:
            return "Under 18"
        elif age < 25:
            return "18-24"
        elif age < 35:
            return "25-34"
        elif age < 45:
            return "35-44"
        elif age < 55:
            return "45-54"
        else:
            return "55+"

    df["age_group"] = df["age"].apply(get_age_group)
    df["club_member_status"] = df.get("club_member_status", pd.Series("Unknown", index=df.index)).fillna("Unknown")
    df["fashion_news_frequency"] = df.get("fashion_news_frequency", pd.Series("Unknown", index=df.index)).fillna("Unknown")
    df["fn"] = pd.to_numeric(df.get("fn"), errors="coerce")
    df["active"] = pd.to_numeric(df.get("active"), errors="coerce")

    print(f"  customers after cleaning: {len(df)}")
    return df


def clean_transactions(df):
    print("Cleaning transactions data...")
    df = df.copy()
    df.columns = [col.lower().strip() for col in df.columns]
    df["t_dat"] = pd.to_datetime(df["t_dat"], errors="coerce")
    df["article_id"] = df["article_id"].astype(str).str.zfill(10)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["sales_channel_rcd"] = pd.to_numeric(df.get("sales_channel_rcd"), errors="coerce")
    df = df.dropna(subset=["t_dat", "customer_id", "article_id", "price"])
    df["source_file"] = "transactions_train.csv"

    print(f"  transactions after cleaning: {len(df)}")
    return df


def build_date_table(transaction_dates):
    print("Building date dimension...")
    all_dates = pd.Series(pd.to_datetime(list(transaction_dates))).dt.normalize().unique()

    date_rows = []
    for d in sorted(all_dates):
        date_rows.append({
            "date_id": d.date(),
            "year": d.year,
            "quarter": d.quarter,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "week": int(d.strftime("%W")),
            "day_of_week": d.weekday(),
            "day_name": d.strftime("%A"),
            "is_weekend": d.weekday() >= 5
        })

    print(f"  unique dates: {len(date_rows)}")
    return pd.DataFrame(date_rows)


def insert_data(conn, table_name, df, columns):
    data = df[[col for col in columns if col in df.columns]].copy()
    data = data.where(pd.notnull(data), None)
    rows_to_insert = [tuple(row) for row in data.itertuples(index=False)]

    if not rows_to_insert:
        print(f"  no data to insert into {table_name}")
        return

    actual_columns = list(data.columns)
    query = f"INSERT INTO {table_name} ({','.join(actual_columns)}) VALUES %s ON CONFLICT DO NOTHING"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows_to_insert, page_size=2000)
    conn.commit()
    print(f"  inserted {len(rows_to_insert)} rows into {table_name}")


def load_to_database(conn, articles, customers, transactions, date_dim):
    print("\nLoading data into PostgreSQL...")

    insert_data(conn, "dim_articles", articles, [
        "article_id", "product_code", "prod_name", "product_type_no",
        "product_type_name", "product_group_name", "graphical_appearance_no",
        "graphical_appearance_name", "colour_group_code", "colour_group_name",
        "perceived_colour_value_name", "perceived_colour_master_name",
        "department_no", "department_name", "index_code", "index_name",
        "index_group_no", "index_group_name", "section_no", "section_name",
        "garment_group_no", "garment_group_name", "detail_desc"
    ])

    insert_data(conn, "dim_customers", customers, [
        "customer_id", "fn", "active", "club_member_status",
        "fashion_news_frequency", "age", "postal_code", "age_group"
    ])

    insert_data(conn, "dim_date", date_dim, [
        "date_id", "year", "quarter", "month", "month_name",
        "week", "day_of_week", "day_name", "is_weekend"
    ])

    valid_articles = set(articles["article_id"])
    valid_customers = set(customers["customer_id"])
    valid_transactions = transactions[
        transactions["article_id"].isin(valid_articles) &
        transactions["customer_id"].isin(valid_customers)
    ].copy()

    print(f"  valid transactions: {len(valid_transactions)}")
    print("  loading fact_sales table...")
    batch_size = 100000
    total_inserted = 0
    batch = []

    for row in valid_transactions.itertuples(index=False):
        batch.append((
            row.customer_id,
            row.article_id,
            row.t_dat.date(),
            row.price,
            int(row.sales_channel_rcd) if pd.notna(row.sales_channel_rcd) else None,
            row.source_file
        ))

        if len(batch) == batch_size:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO fact_sales (customer_id, article_id, transaction_date, price, sales_channel, source_file) VALUES %s",
                    batch,
                    page_size=5000
                )
            conn.commit()
            total_inserted += len(batch)
            print(f"  inserted {total_inserted:,} rows so far...")
            batch = []


    if batch:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO fact_sales (customer_id, article_id, transaction_date, price, sales_channel, source_file) VALUES %s",
                batch,
                page_size=5000
            )
        conn.commit()
        total_inserted += len(batch)

    print(f"  inserted {total_inserted:,} rows into fact_sales")


def run_analysis(conn):
    print("\nRunning business objective queries...")

    queries = [
        (
            "obj1_avg_price_per_category",
            "Objective 1 - Average Sale Price per Category per Year",
            """SELECT year, index_group_name AS category,
                      COUNT(*) AS num_transactions,
                      ROUND(AVG(price)::NUMERIC, 4) AS avg_unit_price,
                      ROUND(SUM(price)::NUMERIC, 2) AS total_revenue
               FROM v_sales_enriched
               GROUP BY year, index_group_name
               ORDER BY year, total_revenue DESC"""
        ),
        (
            "obj2_top20_articles",
            "Objective 2 - Top 20 Best Selling Articles",
            """SELECT article_id, prod_name, product_type_name, index_group_name,
                      COUNT(*) AS times_purchased,
                      ROUND(SUM(price)::NUMERIC, 2) AS total_revenue,
                      ROUND(AVG(price)::NUMERIC, 4) AS avg_price
               FROM v_sales_enriched
               GROUP BY article_id, prod_name, product_type_name, index_group_name
               ORDER BY times_purchased DESC LIMIT 20"""
        ),
        (
            "obj3_monthly_trend",
            "Objective 3 - Monthly Revenue Trend",
            """SELECT year, month, month_name,
                      COUNT(*) AS num_transactions,
                      COUNT(DISTINCT customer_id) AS unique_customers,
                      ROUND(SUM(price)::NUMERIC, 2) AS total_revenue,
                      ROUND(AVG(price)::NUMERIC, 4) AS avg_price
               FROM v_sales_enriched
               GROUP BY year, month, month_name
               ORDER BY year, month"""
        ),
        (
            "obj4_customer_segments",
            "Objective 4 - Customer Segments by Age and Club Status",
            """SELECT age_group, club_member_status,
                      COUNT(DISTINCT customer_id) AS unique_customers,
                      COUNT(*) AS num_purchases,
                      ROUND(SUM(price)::NUMERIC, 2) AS total_revenue,
                      ROUND(AVG(price)::NUMERIC, 4) AS avg_spend_per_purchase
               FROM v_sales_enriched
               GROUP BY age_group, club_member_status
               ORDER BY total_revenue DESC"""
        ),
        (
            "obj5_online_vs_store",
            "Objective 5 - Online vs In-Store Revenue by Department",
            """SELECT department_name, channel_name,
                      COUNT(*) AS num_transactions,
                      ROUND(SUM(price)::NUMERIC, 2) AS total_revenue,
                      ROUND(AVG(price)::NUMERIC, 4) AS avg_price
               FROM v_sales_enriched
               GROUP BY department_name, channel_name
               ORDER BY department_name, total_revenue DESC"""
        ),
        (
            "obj6_loyalty",
            "Objective 6 - Customer Loyalty and Repeat Purchases",
            """WITH customer_summary AS (
                   SELECT customer_id, age_group, club_member_status,
                          COUNT(*) AS total_purchases,
                          ROUND(SUM(price)::NUMERIC, 2) AS lifetime_value
                   FROM v_sales_enriched
                   GROUP BY customer_id, age_group, club_member_status
               )
               SELECT age_group, club_member_status,
                      COUNT(*) AS total_customers,
                      ROUND(AVG(total_purchases), 2) AS avg_purchases,
                      ROUND(AVG(lifetime_value)::NUMERIC, 2) AS avg_lifetime_value,
                      SUM(CASE WHEN total_purchases > 1 THEN 1 ELSE 0 END) AS repeat_buyers,
                      SUM(CASE WHEN total_purchases > 5 THEN 1 ELSE 0 END) AS loyal_buyers
               FROM customer_summary
               GROUP BY age_group, club_member_status
               ORDER BY avg_lifetime_value DESC"""
        )
    ]

    for filename, title, query in queries:
        result_df = pd.read_sql(query, conn)
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
        print(result_df.to_string(index=False))
        output_path = OUTPUT_FOLDER / f"{filename}.csv"
        result_df.to_csv(output_path, index=False)
        print(f"  saved to {filename}.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true",
                        help="use only 50000 rows for testing")
    args = parser.parse_args()

    print("=" * 50)
    print("  H&M FASHION RETAIL DATA PIPELINE")
    print("  Object Storage + On-Premise -> PostgreSQL")
    print("=" * 50)

    conn = connect_db()
    setup_schema(conn)
    articles, customers, transactions = load_data(use_sample=args.sample)
    articles = clean_articles(articles)
    customers = clean_customers(customers)
    transactions = clean_transactions(transactions)
    date_dim = build_date_table(transactions["t_dat"].dropna())
    load_to_database(conn, articles, customers, transactions, date_dim)
    run_analysis(conn)
    conn.close()

    print("\n" + "=" * 50)
    print("  PIPELINE FINISHED SUCCESSFULLY")
    print("=" * 50)


if __name__ == "__main__":
    main()
