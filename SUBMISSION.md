# NammaMart ETL & ELT Pipeline Assignment

## Part 0: Discovery and Data Profiling

### Three Questions for the Client

1. **Negative quantities:** Are negative or zero quantities genuine returns or cancellations, or 
are they data-entry errors that should be rejected?

2. **Order and payment reconciliation:** How should we handle orders with missing payments, 
payments that do not match the order amount, or multiple payment records for one order?

3. **Late status corrections:** If an order changes from DELIVERED to RETURNED in the incremental file,
 should historical revenue be recalculated, or should both the original and corrected versions be retained for audit purposes?


## Data Issues Log

### orders.csv
- 18 order amount mismatches — Accuracy — Critical
- 10 non-positive quantities — Validity — Critical
- 9 unmatched product references — Referential integrity — Critical
- 5 unmatched store references — Referential integrity — Critical
- 5 invalid order statuses — Validity — Critical
- 8 delivered orders missing delivery time — Completeness — Critical

### payments.csv
- 8 unmatched order references — Referential integrity — Critical
- 301 missing gateway references — Completeness — Warning

### customers.csv
- 4 missing email values — Completeness — Warning

### products.csv
- 3 missing category values — Completeness — Critical

### stores.csv
- 1 missing zone value — Completeness — Critical

### orders_2026-10-01_increment.csv
- 10 order IDs already exist in the base orders file — Uniqueness and consistency — Warning



### Data Profiling Evidence

The detailed profiling results are saved in `include/data_profiling_report.txt`.

The report includes:
- Row counts and column counts for all six CSV files.
- Null values and duplicate-row checks.
- Referential integrity checks across related datasets.
- Business rule validation, including order amounts, quantities, statuses, and delivery times.
- Incremental file analysis, including existing order IDs.

The profiling results will be used to define validation, quarantine, and reconciliation rules for the ETL and ELT pipelines.




## Part A — ETL Implementation and Test Results

### Implemented
- Extracted the five base CSV files and the incremental orders file.
- Validated required fields, timestamps, and numeric business rules.
- Quarantined rejected orders with failure reasons and run IDs.
- Removed duplicate order records, with incremental records taking precedence.
- Masked customer email addresses and phone numbers in the customer dimension.
- Joined orders with customer, product, and store dimensions using left joins.
- Derived order date, net revenue, gross margin, and delivery SLA indicator.
- Loaded fact, dimension, reporting, data-quality, and payment reconciliation tables into DuckDB.

### Latest observed results
- Orders processed: 2,513
- Orders quarantined: 37
- Fact table rows: 2,513
- Orders with missing dimension details: 14
- Payment reconciliation: 2,163 MATCH; 350 MISMATCH

### Limitations and follow-up
- Investigate missing product and store references.
- Review mismatched payments, including pending or failed payments.
- Further validate rerun idempotency and incremental processing.
- Validate the Airflow DAG in the Airflow environment.

## Part B: September ETL vs. ELT Comparison

| Metric | Part A: ETL | Part B: ELT |
|---|---:|---:|
| Distinct orders | 2,356 | 2,377 |
| Net revenue | ₹12,91,917.55 | ₹13,39,932.63 |

For September 2026, the ELT pipeline processed 21 more distinct orders and ₹48,015.08 more net revenue than the ETL pipeline.

The pipelines do not yet produce identical results. The exact cause has not been confirmed. Potential causes include differences in data-quality filtering, incremental updates, and transformation logic. Further validation is required.

The ELT pipeline preserves source records in the raw layer and applies deduplication and data-quality checks in the staging layer before building the marts.


## Part C: Schedule Both DAGs for 9:00 AM IST

**Cron expression:** `0 9 * * 1-6`

**UTC time:** 03:30 AM UTC, equivalent to 9:00 AM IST.

**If start_date were left in UTC:** A 9:00 AM UTC schedule would run at 2:30 PM IST, not 9:00 AM IST.

**If the laptop or Docker is off:** The scheduled run cannot execute while the local Airflow scheduler is unavailable. With `catchup=False`, missed runs are not automatically backfilled after restart. A continuously running Airflow deployment is needed to meet the SLA reliably.

**Why catchup=False:** It prevents Airflow from automatically creating historical runs for missed schedule intervals.

**Retries and timeout:** Both DAGs use 2 retries, a 5-minute retry delay, and a 20-minute execution timeout.
## Part D: Data Quality Dimensions

Implemented data-quality checks for six dimensions: completeness, uniqueness, validity, accuracy, consistency, and timeliness.

### ELT Pipeline
- Implemented SQL-based checks in `include/sql/dq_checks.sql`.
- Recorded check results in `dq_results` with run ID, DAG ID, check name, dimension, severity, rows checked, rows failed, status, and timestamp.
- Tested the ELT DAG successfully.
- Failed checks are recorded in the scorecard for review.

### ETL Pipeline
- Implemented Python-based order validation and quarantine of rejected orders.
- ETL quality results are recorded in `etl_dq_results`.
- Additional work is needed to ensure all six dimensions are consistently recorded in the ETL scorecard.

### Limitations
- Further validation is required to ensure Critical failures are quarantined and Warning rows are loaded and logged consistently across both pipelines.
- Repeated test runs can create multiple scorecard entries; results should be reviewed by `run_id`.
