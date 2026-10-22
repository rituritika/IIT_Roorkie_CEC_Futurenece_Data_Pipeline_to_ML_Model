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
## Part E: Data Governance

### 1. Ownership
The Operations team owns orders and stores; Finance owns payments and revenue reconciliation; Customer Management owns customer data; and Merchandising owns product data. The Data Engineering team owns staging, warehouse and mart tables. The Operations owner and Data Governance team should approve changes to quality rules such as the 120-minute delivery limit, with Finance consulted when revenue reporting is affected.

### 2. Data Dictionary

| Table | Column | Type | Meaning / Source | Allowed values | PII |
|---|---|---|---|---|---|
| Fact: orders | order_id | VARCHAR | Unique order identifier from orders CSV | Non-empty, unique | No |
| Fact: orders | customer_id | VARCHAR | Customer reference from orders CSV | Must exist in customers | No, by itself |
| Fact: orders | order_ts | TIMESTAMP | Order creation time from orders CSV | Valid timestamp; not future-dated | No |
| Fact: orders | quantity | INTEGER | Units ordered from orders CSV | Greater than zero | No |
| Fact: orders | net_revenue | DECIMAL/DOUBLE | Revenue after discount, derived from order data | Non-negative | No |
| Dim: customers | customer_id | VARCHAR | Customer key from customers CSV | Unique | No, by itself |
| Dim: customers | customer_name | VARCHAR | Customer's name from customers CSV | Non-empty | Yes |
| Dim: customers | email / phone | VARCHAR | Customer contact details from customers CSV | Valid email / phone format | Yes |
| Dim: products | product_id | VARCHAR | Product key from products CSV | Unique | No |
| Dim: products | category | VARCHAR | Product category from products CSV | Non-empty | No |
| Dim: stores | store_id | VARCHAR | Store key from stores CSV | Unique | No |
| Dim: stores | store_name / zone | VARCHAR | Store name and service zone from stores CSV | Non-empty | No |
| Fact: payments | payment_id | VARCHAR | Payment transaction key from payments CSV | Unique | No |
| Fact: payments | amount_paid | DECIMAL/DOUBLE | Payment amount from payments CSV | Non-negative | No |

The actual physical types may differ between raw, staging and mart layers because raw ELT columns are retained as text and staging applies type conversions.

### 3. Lineage
For `mart_daily_store_kpis`, a measure such as total net revenue is derived from accepted order records, grouped by order date and store, and ultimately originates from `orders.csv` and its order rows. ETL lineage can be traced through processed and quarantined records; ELT lineage can be traced through raw source metadata (`_source_file`, `_run_id`) and staging transformations. ELT is generally easier for source-level tracing because raw records are preserved, but exact row-level lineage requires retaining a source row identifier or equivalent mapping.

### 4. Personal Data
Customer name, email, phone number and any address or other identifying contact details in the customer dataset are personal data. The pipeline should mask contact details in analytical staging/mart layers while retaining controlled raw data for authorised operational use; masking preserves join and audit utility without exposing the original value broadly. India's Digital Personal Data Protection Act, 2023 and the DPDP Rules, 2025 establish obligations around lawful processing, notice/consent where applicable, safeguards, purpose limitation, data-principal rights and retention; applicable provisions have phased commencement dates. [Official Rules](https://www.meity.gov.in/static/uploads/2025/11/53450e6e5dc0bfa85ebd78686cadad39.pdf)

### 5. Access Control
Only authorised ingestion and platform administrators should access raw data; data engineers may access staging with appropriate controls; business users should primarily access approved marts. Finance should access payment reconciliation and revenue marts, while customer operations may need customer details for legitimate service purposes. Raw customer phone numbers should be restricted to authorised personnel who require them for their work, with access logged and reviewed.

### 6. Retention
A proposed policy is to retain raw operational data for 90 days, quarantined records for 30 days after investigation, and masked analytical customer data for 12 months, unless a legal, contractual or business requirement specifies otherwise. These are proposed policy values, not existing implemented controls. After the approved retention period, data should be securely deleted or irreversibly anonymised, subject to legal holds and audit requirements.

### 7. Secrets
Credentials and webhook URLs such as `SLACK_WEBHOOK_URL` should be stored in Airflow Connections, a secrets manager or environment variables supplied securely at runtime. They must never be hard-coded in DAG files or committed to GitHub because repository users could expose them and use them to access systems or send unauthorised messages. Secrets should be rotated if accidentally exposed.

### 8. Audit
In ETL, an invalid order is written to a run-specific quarantine CSV with its received fields, run ID and failure reason, and rejected rows are excluded from the processed fact/revenue output. In ELT, the original row remains in the raw table, while staging records its data-quality status and failure reason; marts must filter out failed records. For a finance audit, retrieve one actual rejected order, show its original CSV/raw row and the exact rule it violated, then verify that its order ID is absent from the accepted revenue calculation. The audit should be demonstrated with an actual order ID from the current run.

### 9. Change History
If an order was DELIVERED on 3 September and changed to RETURNED on 1 October, an incremental upsert may replace the current order status and alter the current September revenue view, depending on the mart's status and revenue rules. Finance usually needs both the original September close and the corrected current view. Preserve both by storing effective-dated order history or immutable monthly snapshots, recording the change timestamp and run ID rather than overwriting the only historical version.


## Part F: Observability Tests

### 1. Run Audit

The pipeline run audit should maintain one record per DAG run in the `pipeline_runs` table with the following fields:

- `run_id`
- `dag_id`
- `start_time`
- `end_time`
- `rows_extracted`
- `rows_loaded`
- `rows_quarantined`
- `status`

A final audit task using `trigger_rule="all_done"` ensures that audit recording is attempted even when an upstream task fails. This provides visibility into successful and failed pipeline executions. The audit table and final task must be verified in both DAGs before claiming full implementation.

### 2. Observability Experiments

The following table defines the expected behaviour and the evidence to inspect for each experiment. Actual observations should be recorded from the Airflow UI, task logs, and database queries after running the tests.

| # | Experiment | Expected result | Evidence to inspect |
|---|---|---|---|
| 1 | Happy path | Pipeline completes successfully, with extracted, loaded and quarantined row counts recorded. Independent tasks may execute in parallel. | Airflow Graph/Grid, task durations, audit table |
| 2 | Re-run (idempotency) | Re-running the DAG should not create duplicate fact records. ELT raw-table growth depends on the loading strategy. | Fact counts and raw-table counts before and after the run |
| 3 | Incremental load | New records are inserted and corrected orders are updated without duplicating order IDs. | Increment-file results, fact table, insert/update counts |
| 4 | Missing source file | The affected task retries according to its configuration and eventually fails if the file remains unavailable. Dependent tasks may become `upstream_failed`. | Airflow task states, retry logs and failure messages |
| 5 | New bad data | Null customer, negative quantity, unknown store, invalid status and future timestamp should be identified by the applicable validation rules. | Quarantine table, rejection reasons and `dq_results` |
| 6 | Schema change | Renaming `order_amount` to `total` should produce a clear schema or transformation error unless the pipeline explicitly supports the changed schema. | Failed task logs and source schema |
| 7 | Volume anomaly | A run with 50 orders should trigger a warning if the baseline is more than 500 orders and the 90% drop check is implemented. | Volume-check log or quality result |
| 8 | Distribution drift | Multiplying a store's `unit_price` by 100 should be detected if a distribution or anomaly check is configured. The affected mart metrics should be compared before and after. | Store-level mart metrics and quality results |
| 9 | Freshness | Orders whose latest timestamp is 30 days old should trigger a warning if the configured timeliness threshold is exceeded. | Latest `order_ts`, task logs and `dq_results` |
| 10 | Recovery | Clearing a failed task should rerun the selected task and the necessary downstream tasks, while completed upstream tasks generally remain completed unless also cleared. | Airflow Grid and task logs |

**Experiment recording format**

For each experiment, on both ETL and ELT, record:

- **Expected:** The intended pipeline behaviour.
- **Observed:** The actual result, including row counts, task states and error messages.
- **Where I saw it:** Airflow UI, task log, database table or SQL query.
- **What I would change:** The improvement required to make the pipeline more reliable.

Experiments that have not been executed should be marked **Not tested**. Expected results are not evidence that a test passed.

### 3. Five Pillars of Data Observability

| Pillar | Application to NammaMart | Assessment |
|---|---|---|
| Freshness | Monitor the latest order timestamp and time since the last successful pipeline run. | Partially covered by the timeliness check; threshold-based alerts require verification. |
| Volume | Track extracted, loaded and quarantined row counts and detect sudden drops. | Requires a baseline comparison and a volume alert. |
| Schema | Detect missing or renamed columns and incompatible data types. | Must be verified through the schema-change experiment. |
| Distribution | Monitor changes in unit prices, quantities, order statuses and store-level metrics. | Requires explicit distribution checks and thresholds. |
| Lineage | Trace source files through staging and transformations into the final marts. | Supported by pipeline documentation; run-level lineage would benefit from recorded run IDs and source references. |

### 4. Conclusion

The existing data-quality checks provide a foundation for pipeline observability. Freshness, volume, schema and distribution monitoring should be validated through controlled experiments and recorded evidence. Distribution is a likely area for improvement because a large price change may not be detected without explicit anomaly checks. The run audit should be verified for both successful and failed runs so that pipeline health can be assessed without manually inspecting the code.

### 5. Limitations and Next Steps

1. Verify that both DAGs write one audit row per run to `pipeline_runs`.
2. Add and validate a volume-drop check against a known-good baseline.
3. Run each experiment against both DAGs and record actual outcomes.
4. Confirm that invalid records are quarantined with specific rejection reasons.
5. Verify that recovery, retries and failure states are visible through Airflow and the audit table.