# Part A ETL vs. Part B ELT — September 2026

The ETL pipeline processed 2,356 distinct orders with total net revenue of ₹12,91,917.55. The ELT pipeline processed 2,377 distinct orders with total net revenue of ₹13,39,932.63.

The ELT results are higher by 21 distinct orders and ₹48,015.08 in net revenue. This difference indicates that the two pipelines are not yet producing identical September results. Possible reasons include differences in data-quality filtering, incremental updates, and transformation logic. Further validation is required to identify the exact cause and confirm that both pipelines apply equivalent business rules.

The ELT pipeline preserves source records in the raw layer and applies deduplication and quality checks in the staging layer before building the marts.
