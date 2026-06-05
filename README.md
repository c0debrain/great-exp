# Great Expectations 1.8.1 Validation Examples

This repo uses `great_expectations==1.8.1` with YAML-defined rules and YAML checkpoints. It includes both CSV validation and Spark-based SQL validation examples for PostgreSQL JDBC and Iceberg-backed Spark tables.

## Files

- `data/customers_valid.csv`: passes both rules
- `data/customers_invalid.csv`: fails both rules
- `data/orders_valid.csv`: passes its order rules
- `data/orders_invalid.csv`: fails its order rules
- `rules/email_not_null.yml`: `email` cannot be null
- `rules/age_between_18_and_65.yml`: `age` must be between 18 and 65
- `rules/order_id_not_null.yml`: `order_id` cannot be null
- `rules/total_between_1_and_1000.yml`: `total` must be between 1 and 1000
- `checkpoints/customers.yml`: lists the CSV files and rule YAML files to run
- `checkpoints/spark_tables_postgres.yml`: explicit PostgreSQL Spark checkpoint
- `checkpoints/spark_tables_iceberg.yml`: explicit Iceberg Spark checkpoint
- `checkpoints/spark_tables_athena.yml`: explicit Athena JDBC Spark checkpoint
- `config.yml`: local backend settings used to resolve `${...}` values in checkpoints
- `validate.py`: loads the YAML config, builds GX objects in memory, runs a checkpoint, and generates HTML Data Docs
- `reports/`: generated GX HTML report output
- `docker-compose.yml`: starts PostgreSQL for the Spark JDBC example
- `sql/init/01_seed_tables.sql`: seeds 3 PostgreSQL tables
- `sql/iceberg/01_seed_tables.sql`: seeds the same 3 demo tables in Iceberg

## Run

```bash
python3 -m pip install -r requirements.txt
python3 validate.py
```

You can also run one or more checkpoint YAML files:

```bash
python3 validate.py checkpoints/customers.yml
```

The script prints the built-in GX checkpoint report, writes HTML Data Docs to `reports/`, prints the local report URL, and exits with code `0` when every validation passes, otherwise it exits with code `1`.

When you add a new rule, update `rules/*.yml`. When you add a new CSV, update the checkpoint YAML. You do not need to edit a persisted `gx/` project for those changes.

## Spark SQL Examples

This repo now uses three explicit Spark checkpoints:

- `checkpoints/spark_tables_postgres.yml`
- `checkpoints/spark_tables_iceberg.yml`
- `checkpoints/spark_tables_athena.yml`

They validate the same logical datasets and rules, but each checkpoint uses a different backend-specific connection model.

### Add A New Table

To add support for a new table, update the rule files first and then add the table to each backend checkpoint that should validate it.

1. Add new SQL expectation files under `rules/sql/`.
2. Add a new validation entry to each target checkpoint:
   - `checkpoints/spark_tables_postgres.yml`
   - `checkpoints/spark_tables_iceberg.yml`
   - `checkpoints/spark_tables_athena.yml`
3. Point the validation at the backend-specific table name.
4. If this repo's demo data should include the table, update the seed scripts:
   - `sql/init/01_seed_tables.sql` for PostgreSQL
   - `sql/iceberg/01_seed_tables.sql` for local Iceberg
5. If the backend uses table names from `config.yml`, add the new config key there.

Example validation block:

```yaml
  - name: spark_invoices
    type: spark_table
    source: postgres_demo
    table: public.invoices
    rules:
      - rules/sql/invoices_invoice_id_not_null.yml
      - rules/sql/invoices_total_between_1_and_1000.yml
```

Backend-specific notes:

- PostgreSQL: add the validation directly to `checkpoints/spark_tables_postgres.yml` with the JDBC-visible table name such as `public.invoices`.
- Iceberg: add the validation to `checkpoints/spark_tables_iceberg.yml` with the catalog table name such as `demo.sales.invoices`.
- Athena: add the validation to `checkpoints/spark_tables_athena.yml` and, if needed, add a matching `ATHENA_INVOICES_TABLE` key in `config.yml`.

If you want every backend to validate the same logical table, keep the validation name and rule list aligned across all three checkpoints and only vary the physical table identifier.

### PostgreSQL backend

Start PostgreSQL:

```bash
docker compose up -d
```

The PostgreSQL JDBC settings are loaded from `config.yml`:

```bash
POSTGRES_JDBC_URL: jdbc:postgresql://localhost:5432/gx_demo
POSTGRES_USER: gx
POSTGRES_PASSWORD: gx
```

Make sure Java is available locally. On first run, Spark downloads the PostgreSQL JDBC driver declared in the checkpoint.

Run the Spark checkpoint:

```bash
python3 validate.py checkpoints/spark_tables_postgres.yml
```

The PostgreSQL backend validates 3 tables:

- `customers` through Spark JDBC
- `products` through Spark JDBC
- `orders` through Spark JDBC

### Iceberg backend

The Iceberg settings are also loaded from `config.yml`:

```yaml
ICEBERG_SPARK_PACKAGES: org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
ICEBERG_CATALOG_TYPE: hadoop
ICEBERG_WAREHOUSE: warehouse
ICEBERG_CUSTOMERS_TABLE: demo.sales.customers
ICEBERG_PRODUCTS_TABLE: demo.sales.products
ICEBERG_ORDERS_TABLE: demo.sales.orders
```

This default setup is a local Hadoop catalog. Spark creates and reads Iceberg tables directly from the local `warehouse/` path. No separate Iceberg service is running in this mode.

Seed the Iceberg demo tables before running validation:

```bash
spark-sql -f sql/iceberg/01_seed_tables.sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=warehouse
```

Then run the same checkpoint:

```bash
python3 validate.py checkpoints/spark_tables_iceberg.yml
```

The Iceberg backend validates the configured catalog-backed tables instead of JDBC tables. The rules do not change.

The PostgreSQL and Iceberg paths now use different checkpoint files on purpose. That keeps the source type, Spark packages, and table identifiers explicit in each backend-specific YAML.

### Switch To An Actual Iceberg Endpoint

You do not need to change `validate.py` to use a real Iceberg catalog endpoint. The change is in [checkpoints/spark_tables_iceberg.yml](/Users/shahin/Workspace/asx/great-exp/checkpoints/spark_tables_iceberg.yml:1) and, if you want, in `config.yml`.

The current checkpoint uses a local Hadoop catalog:

```yaml
spark:
  config:
    spark.sql.catalog.demo: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.demo.type: ${ICEBERG_CATALOG_TYPE}   # hadoop
    spark.sql.catalog.demo.warehouse: ${ICEBERG_WAREHOUSE}
```

To point Spark at a real Iceberg REST catalog endpoint, change the catalog config to `type: rest` and add the catalog URI. Iceberg's Spark configuration uses `spark.sql.catalog.<name>.type=rest` and `spark.sql.catalog.<name>.uri=<endpoint>`.

Example:

```yaml
spark:
  config:
    spark.jars.packages: ${ICEBERG_SPARK_PACKAGES}
    spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.demo: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.demo.type: rest
    spark.sql.catalog.demo.uri: ${ICEBERG_CATALOG_URI}
    spark.sql.catalog.demo.warehouse: ${ICEBERG_WAREHOUSE}
```

Add the corresponding values to `config.yml`:

```yaml
ICEBERG_CATALOG_URI: http://localhost:8181
ICEBERG_WAREHOUSE: s3://warehouse/
```

If the REST catalog stores data in S3 or MinIO, you will usually need extra catalog properties as well:

```yaml
spark:
  config:
    spark.sql.catalog.demo.io-impl: org.apache.iceberg.aws.s3.S3FileIO
    spark.sql.catalog.demo.s3.endpoint: ${ICEBERG_S3_ENDPOINT}
    spark.sql.catalog.demo.s3.access-key-id: ${ICEBERG_S3_ACCESS_KEY_ID}
    spark.sql.catalog.demo.s3.secret-access-key: ${ICEBERG_S3_SECRET_ACCESS_KEY}
    spark.sql.catalog.demo.s3.path-style-access: true
```

Example `config.yml` values for a local MinIO-backed REST catalog:

```yaml
ICEBERG_CATALOG_URI: http://localhost:8181
ICEBERG_WAREHOUSE: s3://warehouse/
ICEBERG_S3_ENDPOINT: http://localhost:9000
ICEBERG_S3_ACCESS_KEY_ID: admin
ICEBERG_S3_SECRET_ACCESS_KEY: password
```

What stays the same:

- `python3 validate.py checkpoints/spark_tables_iceberg.yml`
- the GX rule files in `rules/sql/`
- the table identifiers like `demo.sales.orders`, assuming the catalog exposes the same namespace and table names

What changes:

- the Spark catalog type from `hadoop` to `rest`
- the catalog URI
- warehouse/storage settings
- possibly the package list if your target catalog or storage backend requires more jars

If your target is Nessie rather than plain Iceberg REST, the checkpoint would use `spark.sql.catalog.demo.type: nessie` plus Nessie-specific settings such as `spark.sql.catalog.demo.uri` and `spark.sql.catalog.demo.ref`.

### Athena backend

The Athena path is a separate JDBC checkpoint:

```bash
python3 validate.py checkpoints/spark_tables_athena.yml
```

This path does not use the PostgreSQL JDBC driver and does not use the Iceberg Spark catalog. It connects to Athena through the official Athena JDBC 3.x driver.

Add the Athena JDBC driver jar locally. The checkpoint expects:

```yaml
ATHENA_JDBC_DRIVER_PATH: drivers/athena-jdbc-3.7.0-with-dependencies.jar
```

Download the Athena JDBC 3.x uber jar from AWS and place it at that path, or change the config value to wherever you store it.

The Athena connection settings live in `config.yml`:

```yaml
ATHENA_JDBC_URL: jdbc:athena://Region=us-east-1;Catalog=AwsDataCatalog;Database=default;WorkGroup=primary;OutputLocation=s3://replace-me-athena-results/;CredentialsProvider=DefaultChain;
ATHENA_CUSTOMERS_TABLE: customers
ATHENA_PRODUCTS_TABLE: products
ATHENA_ORDERS_TABLE: orders
```

Important differences from PostgreSQL:

- the driver class is `com.amazon.athena.jdbc.AthenaDriver`
- the JDBC URL starts with `jdbc:athena://`
- Athena requires AWS authentication rather than database username/password
- Athena requires `Region`
- Athena requires `OutputLocation` unless the selected workgroup already defines one

The Athena checkpoint is only a direct query path. It does not turn Athena into an Iceberg catalog endpoint. If your Athena tables are backed by Iceberg in AWS Glue, Athena can query them, but this checkpoint still connects through Athena JDBC rather than the Spark Iceberg catalog.

Operational notes from AWS:

- keep outbound port `444` open because the JDBC 3.x driver uses it for streaming results
- the IAM principal used by the driver needs the `athena:GetQueryResultsStream` permission

The seeded `orders` table intentionally fails its rules so the report shows a SQL validation failure case.

Example of file-specific rules in the checkpoint:

```yaml
validations:
  - name: customers_valid
    file: data/customers_valid.csv
    rules:
      - rules/email_not_null.yml
      - rules/age_between_18_and_65.yml
  - name: orders_valid
    file: data/orders_valid.csv
    rules:
      - rules/order_id_not_null.yml
      - rules/total_between_1_and_1000.yml
```
