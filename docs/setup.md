# Setup Guide

This guide walks through deploying the YouTube Trending Video Data Pipeline
from scratch in an AWS account.

---

## Prerequisites

- AWS Account
- IAM user with administrator access (or scoped permissions)
- AWS CLI installed and configured
- Python 3.14
- YouTube Data API v3 key (for live ingestion)

---

## Step 1 — Create S3 Buckets

```bash
aws s3 mb s3://abbey-youtube-data-pipeline-landing-zone --region us-east-1
aws s3 mb s3://abbey-youtube-data-pipeline-bronze --region us-east-1
aws s3 mb s3://abbey-youtube-data-pipeline-silver --region us-east-1
aws s3 mb s3://abbey-youtube-data-pipeline-gold --region us-east-1
aws s3 mb s3://abbey-youtube-data-pipeline-quarantine --region us-east-1
aws s3 mb s3://abbey-youtube-data-pipeline-athena-results --region us-east-1
```

Enable EventBridge notifications on the Landing Zone bucket:
```
S3 → abbey-youtube-data-pipeline-landing-zone
→ Properties → Amazon EventBridge → Edit → ON → Save
```

Enable EventBridge notifications on the Silver bucket:
```
S3 → abbey-youtube-data-pipeline-silver
→ Properties → Amazon EventBridge → Edit → ON → Save
```

---

## Step 2 — Create SNS Topic

```
SNS → Topics → Create topic
Type: Standard
Name: abbey-youtube-data-pipeline-alerts
```

Add your email as a subscriber and confirm the subscription email.

---

## Step 3 — Create Glue Databases

```
Glue → Databases → Add database
Name: abbey-youtube-data-pipeline-bronze-db

Glue → Databases → Add database
Name: abbey-youtube-data-pipeline-silver-db

Glue → Databases → Add database
Name: abbey-youtube-data-pipeline-gold-db
```

---

## Step 4 — Create Glue Crawler

```
Glue → Crawlers → Create crawler
Name: abbey-youtube-data-pipeline-bronze-crawler
Data source: s3://abbey-youtube-data-pipeline-bronze/youtube/
Database: abbey-youtube-data-pipeline-bronze-db
```

Advanced options:
```
✅ Create a single schema for each S3 path
Table level: 3
Schema changes: Update the table definition in the data catalog
Deleted objects: Delete tables and partitions from the data catalog
```

---

## Step 5 — Upload Glue Scripts

```bash
aws s3 cp glue_jobs/bronze_to_silver.py \
  s3://aws-glue-assets-{account-id}-us-east-1/scripts/abbey-youtube-data-pipeline-bronze-to-silver.py

aws s3 cp glue_jobs/silver_to_gold.py \
  s3://aws-glue-assets-{account-id}-us-east-1/scripts/abbey-youtube-data-pipeline-silver-to-gold.py
```

Replace `{account-id}` with your AWS account ID.

---

## Step 6 — Create Glue Jobs

**Bronze → Silver job:**
```
Glue → Jobs → Create job
Name: abbey-youtube-data-pipeline-bronze-to-silver
Type: Spark
Glue version: Glue 5.1
Script path: s3://aws-glue-assets-{account-id}-us-east-1/scripts/abbey-youtube-data-pipeline-bronze-to-silver.py
```

Job parameters:
```
--bronze_database  = abbey-youtube-data-pipeline-bronze-db
--bronze_table     = raw_statistics
--silver_bucket    = abbey-youtube-data-pipeline-silver
--silver_database  = abbey-youtube-data-pipeline-silver-db
--silver_table     = clean_statistics
```

**Silver → Gold job:**
```
Glue → Jobs → Create job
Name: abbey-youtube-data-pipeline-silver-to-gold
Type: Spark
Glue version: Glue 5.1
Script path: s3://aws-glue-assets-{account-id}-us-east-1/scripts/abbey-youtube-data-pipeline-silver-to-gold.py
```

Job parameters:
```
--silver_database  = abbey-youtube-data-pipeline-silver-db
--gold_bucket      = abbey-youtube-data-pipeline-gold
--gold_database    = abbey-youtube-data-pipeline-gold-db
```

---

## Step 7 — Create Glue Workflow

```
Glue → Workflows → Create workflow
Name: abbey-youtube-transformation-workflow
Max concurrency: 1
```

Inside the workflow graph:

1. Add trigger: Schedule, daily at 2am UTC (`0 2 * * ? *`)
2. Attach node: Crawler → abbey-youtube-data-pipeline-bronze-crawler
3. Add trigger: Event, crawler SUCCEEDED
4. Attach node: Job → abbey-youtube-data-pipeline-bronze-to-silver

---

## Step 8 — Create Lambda Functions

Deploy each Lambda function from the `lambda/` folder:

| Function | Runtime      | Timeout | Memory |
|---|--------------|---|---|
| abbey-youtube-data-pipeline-api-ingestion | Python 3.14  | 5 min | 512 MB |
| abbey-youtube-data-pipeline-malware-scanning | Python 3.114 | 3 min | 512 MB |
| abbey-youtube-data-pipeline-metadata-validator | Python 3.14  | 3 min | 512 MB |
| abbey-youtube-data-pipeline-router | Python 3.14  | 3 min | 512 MB |
| abbey-data-quality-checks | Python 3.14  | 5 min | 512 MB |

Environment variables for Router Lambda:
```
CLEAN_BUCKET_NAME     = abbey-youtube-data-pipeline-bronze
QUARANTINE_BUCKET_NAME = abbey-youtube-data-pipeline-quarantine
```

Environment variables for Data Quality Lambda:
```
S3_BUCKET_SILVER      = abbey-youtube-data-pipeline-silver
SNS_ALERT_TOPIC_ARN   = arn:aws:sns:us-east-1:{account-id}:abbey-youtube-data-pipeline-alerts

```

---

## Step 9 — Create Step Functions State Machines

**Workflow 1 — Ingestion:**
```
Step Functions → State machines → Create
Name: abbey-youtube-ingestion-workflow
Definition: paste contents of step_functions/ingestion_workflow.json
```

**Workflow 2 — Data Quality:**
```
Step Functions → State machines → Create
Name: abbey-youtube-data-quality-workflow
Definition: paste contents of step_functions/data_quality_workflow.json
```

---

## Step 10 — Create EventBridge Rules

**Rule 1 — Landing Zone triggers Workflow 1:**
```
EventBridge → Rules → Create rule
Name: abbey-youtube-landing-zone-ingestion-rule
Event pattern: see eventbridge/landing_zone_rule.json
Target: Step Functions → abbey-youtube-ingestion-workflow
```

**Rule 2 — Silver S3 triggers Workflow 2:**
```
EventBridge → Rules → Create rule
Name: abbey-silver-s3-to-dq-rule
Event pattern: see eventbridge/silver_to_dq_rule.json
Target: Step Functions → abbey-youtube-data-quality-workflow
```

---

## Step 11 — Create EventBridge Schedule (API ingestion)

```
EventBridge → Scheduler → Create schedule
Name: abbey-youtube-api-daily-fetch
Schedule: Cron 0 1 * * ? *  (1am UTC daily)
Target: Lambda → abbey-youtube-data-pipeline-api-ingestion
```

---

## Step 12 — Configure Athena

```
Athena → Settings → Query result location:
s3://abbey-youtube-data-pipeline-athena-results/
✅ Override client-side settings
```

---

## Step 13 — Verify End to End

Upload a test file to the Landing Zone:
```bash
aws s3 cp test-data/CAvideos.csv \
  s3://abbey-youtube-data-pipeline-landing-zone/youtube/raw_statistics/region=ca/
```

Then verify each stage:
```
1. Step Functions → ingestion workflow → Executions → SUCCEEDED
2. S3 → bronze bucket → file exists
3. Glue → run workflow manually to test transformation
4. Step Functions → data quality workflow → Executions → SUCCEEDED
5. S3 → gold bucket → parquet files exist
6. Athena → SELECT * FROM trending_analytics LIMIT 10 → returns rows
```
