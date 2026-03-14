import os
import argparse
import pandas as pd
from pathlib import Path

# folder paths
BASE = Path(__file__).resolve().parent.parent
OBJECT_STORAGE = BASE / "data" / "object_storage"
ON_PREMISE = BASE / "data" / "on_premise"


# adds a new article to the object storage source
def add_article():
    path = OBJECT_STORAGE / "articles.csv"
    df = pd.read_csv(path, dtype=str)

    # check if test article already added
    if "0999999999" in df["article_id"].values:
        print("test article already exists, skipping")
        return

    new_article = {col: "Unknown" for col in df.columns}
    new_article.update({
        "article_id": "0999999999",
        "product_code": "999999",
        "prod_name": "Professor Test Jacket",
        "product_type_no": "253",
        "product_type_name": "Jacket",
        "product_group_name": "Garment Upper body",
        "graphical_appearance_no": "1010016",
        "graphical_appearance_name": "Solid",
        "colour_group_code": "09",
        "colour_group_name": "Black",
        "perceived_colour_value_name": "Dark",
        "perceived_colour_master_name": "Black",
        "department_no": "1676",
        "department_name": "H&M Man",
        "index_code": "B",
        "index_name": "Menswear",
        "index_group_no": "1",
        "index_group_name": "Garment",
        "section_no": "18",
        "section_name": "Mens Outerwear",
        "garment_group_no": "1002",
        "garment_group_name": "Jacket & Waistcoat",
        "detail_desc": "Test article added to verify pipeline updates"
    })

    updated = pd.concat([df, pd.DataFrame([new_article])], ignore_index=True)
    updated.to_csv(path, index=False)
    print("added new article to articles.csv (object storage)")


# adds a new customer to the object storage source
def add_customer():
    path = OBJECT_STORAGE / "customers.csv"
    df = pd.read_csv(path, dtype=str)

    new_id = "TEST_CUSTOMER_PROF_001"

    if new_id in df["customer_id"].values:
        print("test customer already exists, skipping")
        return

    new_customer = {col: "" for col in df.columns}
    new_customer.update({
        "customer_id": new_id,
        "FN": "1.0",
        "Active": "1.0",
        "club_member_status": "ACTIVE",
        "fashion_news_frequency": "Regularly",
        "age": "30",
        "postal_code": "SE-10001"
    })

    updated = pd.concat([df, pd.DataFrame([new_customer])], ignore_index=True)
    updated.to_csv(path, index=False)
    print(f"added new customer {new_id} to customers.csv (object storage)")


# adds new transactions to the on premise source
def add_transactions():
    path = ON_PREMISE / "transactions_train.csv"
    df = pd.read_csv(path, dtype={"article_id": str, "customer_id": str})

    new_transactions = pd.DataFrame([
        {
            "t_dat": "2020-09-22",
            "customer_id": "TEST_CUSTOMER_PROF_001",
            "article_id": "0999999999",
            "price": 0.0847,
            "sales_channel_rcd": 1
        },
        {
            "t_dat": "2020-09-23",
            "customer_id": "TEST_CUSTOMER_PROF_001",
            "article_id": "0999999999",
            "price": 0.0847,
            "sales_channel_rcd": 2
        }
    ])

    for col in df.columns:
        if col not in new_transactions.columns:
            new_transactions[col] = None

    updated = pd.concat([new_transactions[df.columns], df], ignore_index=True)
    updated.to_csv(path, index=False)
    print(f"added 2 new transactions to transactions_train.csv (on premise)")
    print(f"total rows now: {len(updated)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true",
                        help="use sample mode when rerunning pipeline")
    args = parser.parse_args()

    print("=" * 50)
    print("  ADDING NEW DATA TO BOTH SOURCES")
    print("=" * 50)

    # add to object storage source
    add_article()
    add_customer()

    # add to on premise source
    add_transactions()

    # rerun the pipeline to show outputs update
    print("\nrerunning pipeline with new data...")
    flag = "--sample" if args.sample else ""
    os.system(f"python {BASE / 'pipeline' / 'etl_pipeline.py'} {flag}")


if __name__ == "__main__":
    main()
