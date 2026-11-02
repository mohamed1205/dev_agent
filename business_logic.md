<!--
Business rules and quality standards your pipelines must meet.

The governance agent checks every pipeline against these rules, from the data lake to the gold layer,
by reading the pipeline's code and the tables' schemas (never the data). It proposes a fix for any
pipeline that doesn't meet one. The pipeline builder also builds new pipelines to meet them.

Write each rule as a bullet, in plain words, below this comment. Name the tables or columns it covers
when you can. While this file holds only comments, the check is skipped.

Changes to this file go through a normal PR, and its code owners review them.
-->

## Branch sales (Finance)

- transaction_id is unique in sales.pos_transactions: a file the POS vendor sends twice must not count twice
  - Example: the same transaction_id RUH-20261102-0001 in two files of the same day is counted once
- Refunds count as negative sales: daily revenue includes REFUND rows with their negative amount
  - Example: a sale of 100.00 SAR and a refund of -30.00 SAR on the same day give revenue 70.00 SAR
- amount is never null in sales.pos_transactions, and is stored as an exact decimal (NUMERIC) in SAR
- branch_code is one of RUH, JED, DMM in sales.pos_transactions
- transaction_date is never null in sales.pos_transactions, whatever date format the branch's POS sends
- Customer phone numbers never reach the gold layer (sales.pos_transactions, sales.daily_branch_sales)
- Customer phone numbers never reach the AWS export for Finance HQ
- sales.daily_branch_sales has one row per branch and day, and is refreshed every day by 04:00 UTC
- In Finance HQ's AWS copy (finance_hq.daily_branch_sales), branch_code is one of RUH, JED, DMM, revenue_sar is never null, and there is one row per branch and day
