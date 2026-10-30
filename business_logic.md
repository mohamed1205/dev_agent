<!--
Business rules and quality standards your pipelines must meet.

The governance agent checks every pipeline against these rules, from the data lake to the gold layer,
by reading the pipeline's code and the tables' schemas (never the data). It proposes a fix for any
pipeline that doesn't meet one. The pipeline builder also builds new pipelines to meet them.

Write each rule as a bullet, in plain words, below this comment. Name the tables or columns it covers
when you can. While this file holds only comments, the check is skipped.

Examples:
- order_id is never null, and is unique, in every sales table.
- Amounts are stored as exact decimals with 2 places, in EUR.
- Customer emails and phone numbers never reach the gold layer.
- Every gold table is refreshed by 07:00 Europe/Berlin.
- Every order in sales.orders has a matching customer in sales.customers.

Changes to this file go through a normal PR, and its code owners review them.
-->

- order_id is never null in dev_raw.orders

- amount is never negative in dev_raw.orders
  - Example: an order with amount -5.00 is rejected

- order_id is unique in dev_raw.orders_summary
