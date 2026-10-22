
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from datetime import timedelta

from airflow.sdk import dag, task


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "include" / "data" / "nammamart"
QUARANTINE_DIR = BASE_DIR / "include" / "quarantine" / "ritika"
DB_PATH = BASE_DIR / "include" / "nammamart_etl_ritika.duckdb"

INCREMENT_FILE = "orders_2026-10-01_increment.csv"


# ---------- EXTRACTION HELPERS ----------

def extract_csv(filename, include_increment=False):
    """Read one source file and return a JSON payload for Airflow XCom."""
    path = DATA_DIR / filename

    if not path.exists():
        raise FileNotFoundError(f"Missing source file: {path}")

    frame = pd.read_csv(path)

    # Incremental orders are included in the orders extraction task.
    if include_increment:
        increment_path = DATA_DIR / INCREMENT_FILE

        if not increment_path.exists():
            raise FileNotFoundError(
                f"Missing incremental file: {increment_path}"
            )

        increment = pd.read_csv(increment_path)
        frame = pd.concat([frame, increment], ignore_index=True)

    # Standardize whitespace in string columns.
    for column in frame.select_dtypes(include=["object", "string"]).columns:
        frame[column] = frame[column].apply(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    # Normalize common payment method variants.
    for column in ("payment_method", "payment_mode"):
        if column in frame.columns:
            frame[column] = (
                frame[column]
                .astype("string")
                .str.strip()
                .str.upper()
            )

    return frame.to_json(orient="split", date_format="iso")


def decode_frame(payload):
    """Convert an extracted XCom JSON payload back to a DataFrame."""
    return pd.read_json(StringIO(payload), orient="split")


# ---------- TRANSFORM AND LOAD ----------

def run_etl(
    orders_payload,
    payments_payload,
    customers_payload,
    products_payload,
    stores_payload,
):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    orders = decode_frame(orders_payload)
    payments = decode_frame(payments_payload)
    customers = decode_frame(customers_payload)
    products = decode_frame(products_payload)
    stores = decode_frame(stores_payload)

    # 1. Deduplicate orders. Incremental records take precedence.
    orders = orders.drop_duplicates(keep="last")
    orders = orders.drop_duplicates(
        subset=["order_id"],
        keep="last",
    ).copy()

    # 2. Validate orders.
    numeric_columns = [
        "quantity",
        "unit_price",
        "discount_pct",
        "order_amount",
    ]

    for column in numeric_columns:
        orders[column] = pd.to_numeric(
            orders[column],
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
        "duplicate_order_id": orders["order_id"].duplicated(keep="first"),
        "future_order_timestamp": orders["order_ts"] > pd.Timestamp.now(),
        "invalid_order_status": ~orders["order_status"].isin([
            "PLACED", "CONFIRMED", "PREPARING",
            "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"
        ]),
    }

    failed_checks = pd.DataFrame(checks, index=orders.index)
    valid_mask = ~failed_checks.any(axis=1)

    failure_reasons = failed_checks.apply(
        lambda row: ";".join(row.index[row].tolist()),
        axis=1,
    )

    rejected = orders.loc[~valid_mask].copy()
    rejected["failure_reason"] = failure_reasons.loc[~valid_mask]
    rejected["run_id"] = run_id

    if not rejected.empty:
        rejected.to_csv(
            QUARANTINE_DIR / f"orders_failed_{run_id}.csv",
            index=False,
        )

    clean_orders = orders.loc[valid_mask].copy()

    # 3. Derive order measures.
    clean_orders["order_date"] = (
        clean_orders["order_ts"].dt.strftime("%Y-%m-%d")
    )

    clean_orders["net_revenue"] = (
        clean_orders["quantity"]
        * clean_orders["unit_price"]
        * (1 - clean_orders["discount_pct"] / 100)
    ).round(2)

    # 4. Prepare dimensions and remove duplicate business keys.
    customers = customers.drop_duplicates(
        subset=["customer_id"],
        keep="last",
    ).copy()

    products = products.drop_duplicates(
        subset=["product_id"],
        keep="last",
    ).copy()

    stores = stores.drop_duplicates(
        subset=["store_id"],
        keep="last",
    ).copy()

    # Mask customer personal information.
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

    if "email" in customers.columns:
        customers["email"] = customers["email"].apply(mask_email)

    if "phone" in customers.columns:
        customers["phone"] = customers["phone"].apply(mask_phone)

    # 5. Build the order fact table with left joins.
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

    # Identify missing dimension records.
    fact["dimension_quality"] = "PASS"

    missing_customer = fact["full_name"].isna()
    missing_product = fact["product_name"].isna()
    missing_store = fact["store_name"].isna()

    fact.loc[missing_customer, "dimension_quality"] = "MISSING_CUSTOMER"
    fact.loc[missing_product, "dimension_quality"] = "MISSING_PRODUCT"
    fact.loc[missing_store, "dimension_quality"] = "MISSING_STORE"

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

    # 6. Reconcile orders with payments.
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

    reconciliation["reconciliation_reason"] = reconciliation.apply(
        lambda row: (
            "No payment recorded"
            if row["payment_count"] == 0
            else (
                "Payment total differs from order amount "
                f"by {row['amount_difference']:.2f}"
            )
        ),
        axis=1,
    )

    # Assignment requirement: store only mismatches, with a reason.
    reconciliation = reconciliation.loc[
        reconciliation["reconciliation_status"] == "MISMATCH"
    ].copy()

    # 7. Load the warehouse tables idempotently.
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
            try:
                con.execute(
                    f'CREATE OR REPLACE TABLE "{table_name}" AS '
                    "SELECT * FROM source_frame"
                )
            finally:
                con.unregister("source_frame")

        con.execute(
            """
            CREATE OR REPLACE TABLE etl_dq_results AS
            SELECT
                ? AS run_id,
                'nammamart_etl_ritika' AS dag_id,
                'order_validation' AS check_name,
                'validity' AS dimension,
                'ERROR' AS severity,
                ? AS rows_checked,
                ? AS rows_failed,
                CASE WHEN ? = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
                CURRENT_TIMESTAMP AS checked_at
            """,
            [
                run_id,
                len(orders),
                len(rejected),
                len(rejected),
            ],
        )

    print("ETL run:", run_id)
    print("Orders after deduplication:", len(orders))
    print("Orders processed:", len(clean_orders))
    print("Orders quarantined:", len(rejected))
    print("Joined fact rows:", len(fact))
    print("Payment mismatches:", len(reconciliation))
    print("DuckDB database:", DB_PATH)


# ---------- REPORTING ----------

def build_reports():
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute(
            """
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                order_date,
                zone,
                SUM(net_revenue) AS daily_revenue,
                COUNT(*) AS order_count
            FROM etl_fact_orders
            GROUP BY order_date, zone
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                COALESCE(category, 'Uncategorized') AS category,
                SUM(net_revenue) AS net_revenue,
                SUM(gross_margin) AS gross_margin
            FROM etl_fact_orders
            GROUP BY COALESCE(category, 'Uncategorized')
            """
        )

        print("Daily zone revenue report:")
        print(
            con.execute(
                """
                SELECT * FROM etl_daily_zone_revenue
                ORDER BY order_date, zone
                """
            ).fetchdf().to_string(index=False)
        )

        print("Category margin report:")
        print(
            con.execute(
                """
                SELECT * FROM etl_category_margin
                ORDER BY category
                """
            ).fetchdf().to_string(index=False)
        )


# ---------- AIRFLOW DAG ----------

@dag(
    dag_id="nammamart_etl_ritika",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "ritika",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl():

    @task
    def extract_orders():
        return extract_csv("orders.csv", include_increment=True)

    @task
    def extract_payments():
        return extract_csv("payments.csv")

    @task
    def extract_customers():
        return extract_csv("customers.csv")

    @task
    def extract_products():
        return extract_csv("products.csv")

    @task
    def extract_stores():
        return extract_csv("stores.csv")

    @task
    def transform_and_load(
        orders_payload,
        payments_payload,
        customers_payload,
        products_payload,
        stores_payload,
    ):
        run_etl(
            orders_payload,
            payments_payload,
            customers_payload,
            products_payload,
            stores_payload,
        )
        return "load_complete"

    @task
    def reporting(load_status):
        build_reports()

    loaded = transform_and_load(
        extract_orders(),
        extract_payments(),
        extract_customers(),
        extract_products(),
        extract_stores(),
    )

    reporting(loaded)


nammamart_etl()
