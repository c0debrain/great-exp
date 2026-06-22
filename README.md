# Athena Great Expectations Validation

This repo validates Athena tables with `great_expectations==1.8.1`. Rules and checkpoints are YAML-defined, and `validate.py` builds the Great Expectations objects in memory for each run.

## Files

- `checkpoints/athena.yml`: real AWS Athena checkpoint through SQLAlchemy/PyAthena
- `checkpoints/athena_ministack.yml`: local MiniStack Athena checkpoint through SQLAlchemy/PyAthena
- `rules/yml/*.yml`: YAML-defined Athena table expectation rules
- `rules/python/*.py`: Python-defined GX expectation rules for richer checks
- `config.yml`: default values used to resolve `${...}` placeholders
- `docker-compose.yml`: starts MiniStack's full image for local Athena checks
- `ministack/athena-data/`: JSON seed data uploaded into MiniStack S3
- `ministack/init/ready.d/10_seed_athena.py`: creates S3 buckets, registers Glue tables, and smoke-tests Athena
- `validate.py`: runs checkpoints and writes GX, Allure, and JUnit reports

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

## Reports

Great Expectations Data Docs are written to:

```text
reports/index.html
```

Allure result files are written to:

```text
allure-results/
```

Allure HTML is generated automatically when `allure` or `npx` is available:

```text
allure-report/index.html
```

JUnit XML for Xray import is written to:

```text
junit-report/results.xml
```

JUnit HTML is generated from that XML with `junit2html`:

```text
junit-report/index.html
```

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

Add expectation files under `rules/yml/` or `rules/python/`, then add them to the target checkpoint validation.

Every validation must define `test_key`. This value is written to JUnit XML, added to Allure labels/parameters, and included in each GX expectation's metadata and description so it is visible in generated GX HTML.

```yaml
validations:
  - name: athena_customers
    test_key: TEST-101
    type: sqlalchemy_table
    source: athena_demo
    table: ${ATHENA_CUSTOMERS_TABLE}
    rules:
      - rules/yml/customers_email_not_null.yml
      - rules/python/customers_status_in_set.py
```

The generated JUnit testcase includes:

```xml
<properties>
  <property name="test_key" value="TEST-101" />
</properties>
```

YAML rules map directly to one Great Expectations expectation:

```yaml
name: products_price_between_1_and_1000
description: Product price must be between 1 and 1000 inclusive.
expectation_type: expect_column_values_to_be_between
kwargs:
  column: price
  min_value: 1
  max_value: 1000
```

Python rules define an `expectations()` function and can return one expectation or a list of expectations:

```python
def expectations():
    return {
        "type": "expect_column_values_to_be_in_set",
        "description": "Customer status must be active or inactive.",
        "kwargs": {
            "column": "status",
            "value_set": ["active", "inactive"],
        },
    }
```

Python rule functions may also accept `validation`, `source`, or `env` keyword arguments when a rule needs checkpoint context or resolved config values. For Athena, verify new expectation types against MiniStack because some GX SQLAlchemy metrics are dialect-sensitive.

```python
def expectations(validation, env):
    return {
        "type": "expect_table_row_count_to_be_between",
        "kwargs": {
            "min_value": 1,
        },
    }
```

For cross-table validation, a Python rule can also define `query()`. The query should return the rows that the expectation will validate:

```python
def query(env):
    orders = env("${ATHENA_ORDERS_TABLE}")
    customers = env("${ATHENA_CUSTOMERS_TABLE}")
    return f"""
        SELECT o.order_id, o.customer_id
        FROM {orders} o
        LEFT JOIN {customers} c
            ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """


def expectations():
    return {
        "type": "expect_table_row_count_to_equal",
        "description": "Every order customer_id must exist in customers.",
        "kwargs": {
            "value": 0,
        },
    }
```

If a Python rule defines `query()`, keep it as the only query-owning rule in that validation. Multiple query-owning rules should be split into separate checkpoint validations.
