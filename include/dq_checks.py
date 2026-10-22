from datetime import datetime, timezone

DIMENSIONS = [
    ("completeness", "Critical"),
    ("uniqueness", "Critical"),
    ("validity", "Critical"),
    ("accuracy", "Critical"),
    ("consistency", "Critical"),
    ("timeliness", "Warning"),
]


def record_dq_results(con, run_id, dag_id, checks):
    """
    checks: list of tuples (check_name, dimension, severity,
                            rows_checked, rows_failed)
    """
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

    checked_at = datetime.now(timezone.utc)

    for check_name, dimension, severity, rows_checked, rows_failed in checks:
        status = "PASS" if rows_failed == 0 else "FAIL"
        con.execute("""
            INSERT INTO dq_results
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id, dag_id, check_name, dimension, severity,
            rows_checked, rows_failed, status, checked_at
        ])
