
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from airflow.sdk import dag, task


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "include" / "data" / "nammamart"
QUARANTINE_DIR = BASE_DIR / "include" / "quarantine" / "ritika"
DB_PATH = BASE_DIR / "include" / "nammamart_etl_ritika.duckdb"


BASE_FILES = [
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
]


def run_etl():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. EXTRACT source files
    data = {}

    for filename in BASE_FILES:
        path = DATA_DIR / filename

        if not path.exists():
            raise FileNotFoundError(f"Missing source: {path}")

        data[filename.removesuffix(".csv")] = pd.read_csv(path)

    increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"

    if not increment_path.exists():
        raise FileNotFoundError(f"Missing source: {increment_path}")

    increment = pd.read_csv(increment_path)

    # 2. COMBINE orders; incremental records take precedence
    orders = pd.concat(
        [data["orders"], increment],
        ignore_index=True,
    )

    orders = orders.drop_duplicates(keep="last")
    orders = orders.drop_duplicates(
        subset=["order_id"],
        keep="last",
    ).copy()

    # 3. VALIDATE required fields and numeric values
    numeric_cols = [
        "quantity",
        "unit_price",
        "discount_pct",
        "order_amount",
    ]

    for col in numeric_cols:
        orders[col] = pd.to_numeric(
            orders[col],
            errors="coerce",
        )

    orders["order_ts"] = pd.to_datetime(
        orders["order_ts"],
        errors="coerce",
    )

    checks = {
        "missing_order_id": orders["order_id"].isna(),
        "missing_customer_id": orders["customer_id"].isna(),
        "missing_store_id": orders["store_id"].isna(),
        "missing_product_id": orders["product_id"].isna(),
        "invalid_order_timestamp": orders["order_ts"].isna(),
        "invalid_quantity": ~orders["quantity"].gt(0),
        "invalid_unit_price": ~orders["unit_price"].ge(0),
        "invalid_discount": ~orders["discount_pct"].between(0, 100),
        "invalid_order_amount": ~orders["order_amount"].ge(0),
    }

    failed = pd.DataFrame(checks, index=orders.index)
    valid = ~failed.any(axis=1)

    reasons = failed.apply(
        lambda row: ";".join(row.index[row].tolist()),
        axis=1,
    )

    rejected = orders.loc[~valid].copy()
    rejected["failure_reason"] = reasons.loc[~valid]
    rejected["run_id"] = run_id

    if not rejected.empty:
        rejected.to_csv(
            QUARANTINE_DIR / f"orders_failed_{run_id}.csv",
            index=False,
        )

    clean_orders = orders.loc[valid].copy()

    # 4. STANDARDIZE and derive order measures
    clean_orders["order_date"] = (
        clean_orders["order_ts"].dt.strftime("%Y-%m-%d")
    )

    clean_orders["net_revenue"] = (
        clean_orders["quantity"]
        * clean_orders["unit_price"]
        * (1 - clean_orders["discount_pct"] / 100)
    ).round(2)

    # 5. PREPARE dimension tables
    customers = data["customers"].drop_duplicates(
        subset=["customer_id"],
        keep="last",
    ).copy()

    products = data["products"].drop_duplicates(
        subset=["product_id"],
        keep="last",
    ).copy()

    stores = data["stores"].drop_duplicates(
        subset=["store_id"],
        keep="last",
    ).copy()

    # Mask customer email and phone in the dimension
    def mask_email(value):
        if pd.isna(value) or "@" not in str(value):
            return None

        name, domain = str(value).split("@", 1)
        return name[:1] + "***@" + domain

    def mask_phone(value):
        if pd.isna(value):
            return None

        digits = "".join(ch for ch in str(value) if ch.isdigit())
        return "***" + digits[-4:] if digits else None

    customers["email"] = customers["email"].apply(mask_email)
    customers["phone"] = customers["phone"].apply(mask_phone)

    # 6. JOIN using LEFT JOIN behavior
    # Retain orders even if a dimension record is missing.
    fact = (
        clean_orders
        .merge(
            customers,
            on="customer_id",
            how="left",
            suffixes=("", "_customer"),
        )
        .merge(
            products,
            on="product_id",
            how="left",
            suffixes=("", "_product"),
        )
        .merge(
            stores,
            on="store_id",
            how="left",
            suffixes=("", "_store"),
        )
    )

    fact["dimension_quality"] = "PASS"

    missing_customer = fact["full_name"].isna()
    missing_product = fact["product_name"].isna()
    missing_store = fact["store_name"].isna()

    fact.loc[missing_customer, "dimension_quality"] = (
        "MISSING_CUSTOMER"
    )
    fact.loc[missing_product, "dimension_quality"] = (
        "MISSING_PRODUCT"
    )
    fact.loc[missing_store, "dimension_quality"] = (
        "MISSING_STORE"
    )

    fact.loc[
        missing_product & missing_store,
        "dimension_quality",
    ] = "MISSING_PRODUCT_AND_STORE"

    fact["gross_margin"] = (
        fact["net_revenue"]
        - fact["quantity"] * fact["cost_price"]
    ).round(2)

    fact["within_15_min"] = (
        pd.to_numeric(
            fact["delivery_minutes"],
            errors="coerce",
        ).le(15)
    )

    # 7. PAYMENT RECONCILIATION
    payments = data["payments"].copy()

    payments["amount_paid"] = pd.to_numeric(
        payments["amount_paid"],
        errors="coerce",
    )

    payment_summary = (
        payments.groupby("order_id", dropna=False)
        .agg(
            total_paid=("amount_paid", "sum"),
            payment_count=("payment_id", "count"),
        )
        .reset_index()
    )

    reconciliation = (
        clean_orders[
            ["order_id", "order_amount", "order_status"]
        ]
        .merge(
            payment_summary,
            on="order_id",
            how="left",
        )
    )

    reconciliation["payment_count"] = (
        reconciliation["payment_count"].fillna(0).astype(int)
    )

    reconciliation["total_paid"] = (
        reconciliation["total_paid"].fillna(0).round(2)
    )

    reconciliation["amount_difference"] = (
        reconciliation["order_amount"]
        - reconciliation["total_paid"]
    ).round(2)

    reconciliation["reconciliation_status"] = (
        reconciliation["amount_difference"]
        .abs()
        .le(0.01)
        .map({True: "MATCH", False: "MISMATCH"})
    )

    # 8. LOAD tables into DuckDB
    with duckdb.connect(str(DB_PATH)) as con:
        tables = [
            ("etl_dim_customer", customers),
            ("etl_dim_product", products),
            ("etl_dim_store", stores),
            ("etl_fact_orders", fact),
            ("etl_payment_reconciliation", reconciliation),
        ]

        for table_name, frame in tables:
            con.register("source_frame", frame)

            con.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS '
                "SELECT * FROM source_frame"
            )

            con.unregister("source_frame")

        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                order_date,
                zone,
                SUM(net_revenue) AS daily_revenue,
                COUNT(*) AS order_count
            FROM etl_fact_orders
            GROUP BY order_date, zone
        """)

        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                category,
                SUM(net_revenue) AS net_revenue,
                SUM(gross_margin) AS gross_margin
            FROM etl_fact_orders
            GROUP BY category
        """)

        con.execute("""
            CREATE OR REPLACE TABLE etl_dq_results AS
            SELECT
                ? AS run_id,
                'nammamart_etl_ritika' AS dag_id,
                'order_validation' AS check_name,
                'validity' AS dimension,
                'ERROR' AS severity,
                ? AS rows_checked,
                ? AS rows_failed,
                CASE
                    WHEN ? = 0 THEN 'PASS'
                    ELSE 'FAIL'
                END AS status,
                CURRENT_TIMESTAMP AS checked_at
        """, [
            run_id,
            len(orders),
            len(rejected),
            len(rejected),
        ])

        print("ETL run:", run_id)
        print("Orders after deduplication:", len(orders))
        print("Orders processed:", len(clean_orders))
        print("Orders quarantined:", len(rejected))
        print("Joined fact rows:", len(fact))
        print(
            "Orders with missing dimensions:",
            int((fact["dimension_quality"] != "PASS").sum()),
        )
        print("DuckDB database:", DB_PATH)


@dag(
    dag_id="nammamart_etl_ritika",
    schedule="0 9 * * 1-6",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={
        "owner": "ritika",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl():
    @task
    def extract_transform_load():
        run_etl()

    extract_transform_load()


nammamart_etl()
