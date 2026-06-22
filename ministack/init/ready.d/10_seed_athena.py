#!/usr/bin/env python3
import os
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION_NAME = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID = os.getenv("MINISTACK_ACCOUNT_ID", "000000000000")
S3_DATA_DIR = Path(os.getenv("S3_DATA_DIR", "/tmp/ministack-data/s3"))
DATA_BUCKET = "gx-demo-athena-data"
RESULTS_BUCKET = "gx-demo-athena-results"
DATABASE_NAME = "default"
SEED_ROOT = Path("/seed/athena-data")


def client(service):
    kwargs = {}
    if service == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION_NAME,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        **kwargs,
    )


def ensure_bucket(s3, bucket):
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def write_seed_data():
    for data_file in SEED_ROOT.glob("*/*"):
        if data_file.is_file():
            key = f"{data_file.parent.name}/{data_file.name}"
            target = S3_DATA_DIR / ACCOUNT_ID / DATA_BUCKET / key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data_file.read_bytes())


def ensure_database(glue):
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE_NAME})
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "AlreadyExistsException":
            raise


def replace_table(glue, table_name, columns):
    try:
        glue.delete_table(DatabaseName=DATABASE_NAME, Name=table_name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise

    glue.create_table(
        DatabaseName=DATABASE_NAME,
        TableInput={
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "json",
            },
            "StorageDescriptor": {
                "Columns": columns,
                "Location": f"s3://{DATA_BUCKET}/{table_name}/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {},
                },
            },
        },
    )


def wait_for_query(athena, query_id):
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"MiniStack Athena query {query_id} {state}: {reason}")
        time.sleep(0.2)


def smoke_test_athena(athena):
    response = athena.start_query_execution(
        QueryString="SELECT COUNT(*) AS row_count FROM customers",
        QueryExecutionContext={"Database": DATABASE_NAME, "Catalog": "AwsDataCatalog"},
        WorkGroup="primary",
        ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/"},
    )
    wait_for_query(athena, response["QueryExecutionId"])


def main():
    s3 = client("s3")
    glue = client("glue")
    athena = client("athena")

    ensure_bucket(s3, DATA_BUCKET)
    ensure_bucket(s3, RESULTS_BUCKET)
    write_seed_data()
    ensure_database(glue)

    replace_table(
        glue,
        "customers",
        [
            {"Name": "customer_id", "Type": "int"},
            {"Name": "email", "Type": "string"},
            {"Name": "status", "Type": "string"},
        ],
    )
    replace_table(
        glue,
        "products",
        [
            {"Name": "product_id", "Type": "int"},
            {"Name": "sku", "Type": "string"},
            {"Name": "price", "Type": "double"},
        ],
    )
    replace_table(
        glue,
        "orders",
        [
            {"Name": "order_id", "Type": "int"},
            {"Name": "customer_id", "Type": "int"},
            {"Name": "total", "Type": "double"},
        ],
    )
    smoke_test_athena(athena)
    print("MiniStack Athena seed complete.")


if __name__ == "__main__":
    main()
