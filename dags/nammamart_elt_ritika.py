
from pathlib import Path
from datetime import datetime, timezone
import csv
import duckdb
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "include/data/nammamart"
SQL = BASE / "include/sql"
DB = BASE / "include/nammamart_etl_ritika.duckdb"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

@dag(
    dag_id="nammamart_elt_ritika",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["nammamart", "elt"],
)
def nammamart_elt_ritika():

    @task
    def extract_load_raw():
        run_id = get_current_context()["run_id"]
        con = duckdb.connect(str(DB))
        try:
            for table, filename in FILES.items():
                sources = [filename]
                if table == "orders":
                    sources.append("orders_2026-10-01_increment.csv")

                for source in sources:
                    raw_table = f"raw_{table}"
                    already_loaded = con.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables "
                        f"WHERE table_name = ?",
                        [raw_table],
                    ).fetchone()[0]

                    if already_loaded:
                        loaded_count = con.execute(
                            f"SELECT COUNT(*) FROM {raw_table} "
                            f"WHERE _source_file = ?",
                            [source],
                        ).fetchone()[0]
                        if loaded_count > 0:
                            print(source, "already loaded; skipping")
                            continue

                    with (DATA / source).open(
                        encoding="utf-8-sig", newline=""
                    ) as f:
                        reader = csv.DictReader(f)
                        cols = reader.fieldnames
                        if not cols:
                            continue

                        con.execute(
                            f"CREATE TABLE IF NOT EXISTS raw_{table} ("
                            + ", ".join(f'"{c}" VARCHAR' for c in cols)
                            + ", _loaded_at TIMESTAMP, "
                              "_source_file VARCHAR, _run_id VARCHAR)"
                        )
                        rows = [
                            [row.get(c) for c in cols]
                            + [datetime.now(timezone.utc).replace(tzinfo=None),
                               source, run_id]
                            for row in reader
                        ]
                        if rows:
                            con.executemany(
                                f"INSERT INTO raw_{table} VALUES ("
                                + ",".join(["?"] * (len(cols) + 3)) + ")",
                                rows,
                            )
                        print(source, "raw rows loaded:", len(rows))
        finally:
            con.close()

    @task
    def transform_staging():
        con = duckdb.connect(str(DB))
        try:
            for name in [
                "stg_customers.sql", "stg_products.sql",
                "stg_stores.sql", "stg_payments.sql",
                "stg_orders.sql",
            ]:
                con.execute((SQL / name).read_text())
                print("Executed", name)
                            # Run Part D data-quality checks
            dq_sql = (SQL / "dq_checks.sql").read_text()
            dq_results = con.execute(dq_sql).fetchall()

            run_id = str(get_current_context()["run_id"])

            con.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    check_name VARCHAR,
                    dimension VARCHAR,
                    severity VARCHAR,
                    rows_checked INTEGER,
                    rows_failed INTEGER,
                    status VARCHAR,
                    checked_at TIMESTAMPTZ
                )
            """)

            for check_name, dimension, severity, rows_checked, rows_failed in dq_results:
                con.execute("""
                    INSERT INTO dq_results
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    run_id,
                    "nammamart_elt_ritika",
                    check_name,
                    dimension,
                    severity,
                    rows_checked,
                    rows_failed,
                    "PASS" if rows_failed == 0 else "FAIL",
                ])

            print("Recorded", len(dq_results), "data-quality checks")
        finally:
            con.close()

    @task
    def build_marts():
        con = duckdb.connect(str(DB))
        try:
            for name in [
                "mart_daily_store_kpis.sql",
                "mart_category_margin.sql",
                "mart_delivery_sla.sql",
                "mart_payment_reconciliation.sql",
                "mart_top_customers.sql",
            ]:
                con.execute((SQL / name).read_text())
                print("Executed", name)

            for table in [
                "mart_daily_store_kpis", "mart_category_margin",
                "mart_delivery_sla", "mart_payment_reconciliation",
                "mart_top_customers",
            ]:
                print("\n", table)
                print(con.execute(
                    f"SELECT * FROM {table} LIMIT 10"
                ).fetchdf())
        finally:
            con.close()

    raw = extract_load_raw()
    staging = transform_staging()
    marts = build_marts()
    raw >> staging >> marts

nammamart_elt_ritika()
