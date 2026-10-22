
import pandas as pd
from pathlib import Path

BASE = Path("include/data/nammamart")
REPORT = Path("include/data_profiling_report.txt")

files = {
    p.stem: pd.read_csv(p)
    for p in sorted(BASE.glob("*.csv"))
}

lines = []

def log(message=""):
    print(message)
    lines.append(str(message))

log("NAMMAMART - PART 0 DATA PROFILING REPORT")
log("=" * 55)

# 1. Profile all six CSV files
for name, df in files.items():
    log(f"\nFILE: {name}.csv")
    log(f"ROWS: {len(df)} | COLUMNS: {len(df.columns)}")
    log("NULLS PER COLUMN:")
    log(df.isna().sum().to_string())
    log(f"EXACT DUPLICATE ROWS: {df.duplicated().sum()}")

    for col in df.columns:
        if col.endswith("_id"):
            log(f"DUPLICATE KEY {col}: {df[col].duplicated().sum()}")

    for col in df.select_dtypes(include="number").columns:
        log(f"{col} MIN={df[col].min()} MAX={df[col].max()}")

    for col in df.columns:
        if col in [
            "order_ts", "payment_ts", "signup_date",
            "launch_date", "open_date"
        ]:
            dates = pd.to_datetime(df[col], errors="coerce")
            log(f"{col} DATE MIN={dates.min()} MAX={dates.max()}")

    for col in [
        "order_status", "payment_mode", "loyalty_tier",
        "zone", "category"
    ]:
        if col in df.columns:
            log(f"{col} DISTINCT: {df[col].value_counts(dropna=False).to_dict()}")

# 2. Referential integrity checks
log("\n\nREFERENTIAL INTEGRITY")
orders = files["orders"]
customers = files["customers"]
products = files["products"]
stores = files["stores"]
payments = files["payments"]

relationships = [
    ("orders.customer_id", orders, "customer_id", customers, "customer_id"),
    ("orders.product_id", orders, "product_id", products, "product_id"),
    ("orders.store_id", orders, "store_id", stores, "store_id"),
    ("payments.order_id", payments, "order_id", orders, "order_id"),
]

for label, child, ck, parent, pk in relationships:
    bad = child[ck].notna() & ~child[ck].isin(parent[pk])
    log(f"{label}: unmatched references = {int(bad.sum())}")

# 3. Business rule checks on base orders
log("\n\nBUSINESS RULE CHECKS")
expected = (
    orders["quantity"] * orders["unit_price"]
    * (1 - orders["discount_pct"] / 100)
).round(2)
actual = pd.to_numeric(orders["order_amount"], errors="coerce").round(2)

log(f"Order amount mismatches: {int(((expected - actual).abs() > 0.01).sum())}")
log(f"Non-positive quantities: {int((orders['quantity'] <= 0).sum())}")
log(f"Negative unit prices: {int((orders['unit_price'] < 0).sum())}")
log(f"Discount outside 0-100: {int((~orders['discount_pct'].between(0, 100)).sum())}")

valid_status = ["DELIVERED", "CANCELLED", "RETURNED"]
log(f"Invalid order statuses: {int((~orders['order_status'].isin(valid_status) & orders['order_status'].notna()).sum())}")

delivered = orders["order_status"].eq("DELIVERED")
minutes = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
log(f"Delivered orders missing delivery time: {int((delivered & minutes.isna()).sum())}")
log(f"Delivered times outside 8-35 minutes: {int((delivered & ((minutes < 8) | (minutes > 35))).sum())}")

valid_modes = ["UPI", "Card", "Cash on Delivery", "Wallet"]
log(f"Invalid payment modes in orders: {int((~orders['payment_mode'].isin(valid_modes) & orders['payment_mode'].notna()).sum())}")

# 4. Incremental file checks
increment = files["orders_2026-10-01_increment"]
log("\n\nINCREMENTAL FILE")
log(f"Increment rows: {len(increment)}")
log(f"IDs already in base orders: {int(increment['order_id'].isin(orders['order_id']).sum())}")

# 5. Save a report for Part 0
REPORT.write_text("\n".join(lines), encoding="utf-8")
print(f"\nReport saved to: {REPORT}")
