# Athena Great Expectations Validation

This repo validates Athena tables with `great_expectations==1.8.1`. Rules and checkpoints are YAML-defined, and `validate.py` builds the Great Expectations objects in memory for each run.

## Files

- `checkpoints/athena.yml`: real AWS Athena checkpoint through SQLAlchemy/PyAthena
- `checkpoints/athena_ministack.yml`: local MiniStack Athena checkpoint through SQLAlchemy/PyAthena
- `rules/sql/*.yml`: Athena table expectation rules
- `config.yml`: default values used to resolve `${...}` placeholders
- `docker-compose.yml`: starts MiniStack's full image for local Athena checks
- `ministack/athena-data/`: JSON seed data uploaded into MiniStack S3
- `ministack/init/ready.d/10_seed_athena.py`: creates S3 buckets, registers Glue tables, and smoke-tests Athena
- `validate.py`: runs checkpoints and writes Data Docs to `reports/`

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Local Athena With MiniStack

Start MiniStack and seed the local Athena tables:

```bash
docker compose up -d
```

Run the default checkpoint:

```bash
python3 validate.py
```

Equivalent explicit command:

```bash
python3 validate.py checkpoints/athena_ministack.yml
```

The MiniStack stack runs `ministackorg/ministack:full` with `ATHENA_ENGINE=duckdb`. The ready script loads `customers`, `products`, and `orders` tables into MiniStack S3/Glue, then runs an Athena smoke query.

## Real AWS Athena

Run:

```bash
python3 validate.py checkpoints/athena.yml
```

Set these values in `config.yml` or override them through environment variables:

```yaml
ATHENA_REGION_NAME: us-east-1
ATHENA_WORK_GROUP: primary
ATHENA_SCHEMA_NAME: default
ATHENA_CATALOG_NAME: AwsDataCatalog
ATHENA_S3_STAGING_DIR: ""
ATHENA_CUSTOMERS_TABLE: customers
ATHENA_PRODUCTS_TABLE: products
ATHENA_ORDERS_TABLE: orders
```

Authentication comes from the normal boto3/AWS credential chain. Set `ATHENA_S3_STAGING_DIR` when your workgroup does not define query result storage.

## Adding Rules

Add expectation files under `rules/sql/`, then add a validation entry to the target checkpoint with `type: sqlalchemy_table`.
