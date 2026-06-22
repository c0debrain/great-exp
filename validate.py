import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import great_expectations as gx
import pandas as pd
import yaml
from great_expectations import Checkpoint, ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".gx_runtime"
REPORTS = ROOT / "reports"
ALLURE_RESULTS = ROOT / "allure-results"
ALLURE_REPORT = ROOT / "allure-report"
DEFAULT_CHECKPOINT = "checkpoints/athena_ministack.yml"


def load_yaml(path):
    return yaml.safe_load((ROOT / path).read_text())


def load_env():
    config = ROOT / "config.yml"
    if config.exists():
        for key, value in yaml.safe_load(config.read_text()).items():
            os.environ.setdefault(key, str(value))


def env(value):
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        key = value[2:-1]
        if key not in os.environ:
            raise SystemExit(f"Environment variable '{key}' is required.")
        return os.environ[key]
    if isinstance(value, dict):
        return {k: env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [env(v) for v in value]
    return value


def reset_outputs():
    for path in (RUNTIME, REPORTS, ALLURE_RESULTS, ALLURE_REPORT):
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(exist_ok=True)
    (ALLURE_RESULTS / "environment.properties").write_text(
        "framework=great_expectations\nrunner=validate.py\nbackend=athena\n"
    )


def gx_context():
    def fs_store(name):
        return {
            "class_name": name,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(RUNTIME / name.lower().replace("store", "")),
            },
        }

    stores = {
        "expectations_store": fs_store("ExpectationsStore"),
        "validation_results_store": fs_store("ValidationResultsStore"),
        "validation_definition_store": fs_store("ValidationDefinitionStore"),
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "suppress_store_backend_id": True,
                "base_directory": str(RUNTIME / "checkpoints"),
            },
        },
    }
    return gx.get_context(
        project_config=DataContextConfig(
            config_version=4.0,
            stores=stores,
            expectations_store_name="expectations_store",
            validation_results_store_name="validation_results_store",
            checkpoint_store_name="checkpoint_store",
            data_docs_sites={
                "local_site": {
                    "class_name": "SiteBuilder",
                    "show_how_to_buttons": False,
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": str(REPORTS),
                    },
                    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                }
            },
            progress_bars={"globally": False, "metric_calculations": False},
            analytics_enabled=False,
        ),
        mode="ephemeral",
    )


def athena_engine(source):
    region = env(source["region_name"])
    query = {
        k: env(source[k])
        for k in ("catalog_name", "work_group", "s3_staging_dir", "profile_name", "endpoint_url")
        if k in source
    }
    query.update(env(source.get("options", {})))
    url = URL.create(
        drivername=source.get("driver", "awsathena+rest"),
        username=env(source.get("aws_access_key_id", "")),
        password=env(source.get("aws_secret_access_key", "")),
        host=f"athena.{region}.amazonaws.com",
        port=443,
        database=env(source.get("schema_name", "default")),
        query={k: str(v) for k, v in query.items()},
    )
    return create_engine(url)


def athena_frame(source, validation):
    sql = env(validation.get("query") or source.get("query") or f"SELECT * FROM {env(validation['table'])}")
    engine = athena_engine(source)
    try:
        with engine.connect() as connection:
            return pd.read_sql_query(text(sql), connection)
    finally:
        engine.dispose()


def gx_validate(context, validation, dataframe, rules):
    suite = context.suites.add_or_update(
        ExpectationSuite(
            name=f"{validation['name']}_suite",
            expectations=[
                ExpectationConfiguration(
                    type=(rule := load_yaml(path))["expectation_type"],
                    kwargs=rule["kwargs"],
                    description=rule.get("description"),
                    meta={"rule_name": rule["name"], "source_file": Path(path).name},
                )
                for path in validation.get("rules", rules)
            ],
        )
    )
    datasource = context.data_sources.add_pandas(name=f"{validation['name']}_ds")
    asset = datasource.add_dataframe_asset(name=f"{validation['name']}_asset")
    definition = context.validation_definitions.add_or_update(
        ValidationDefinition(
            name=validation["name"],
            data=asset.add_batch_definition_whole_dataframe("default_batch"),
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add_or_update(
        Checkpoint(name=validation["name"], validation_definitions=[definition])
    )
    return checkpoint.run(batch_parameters={"dataframe": dataframe})


def failure_message(result):
    failures = []
    for validation_result in result.describe().get("validation_results", []):
        for expectation in validation_result.get("expectations", []):
            if expectation.get("success"):
                continue
            column = expectation.get("kwargs", {}).get("column")
            count = expectation.get("result", {}).get("unexpected_count")
            failures.append(f"{expectation.get('expectation_type')} on {column}: {count} unexpected")
    return "; ".join(failures) or "Great Expectations validation failed."


def allure_result(validation, status, start, stop, message=None, trace=None):
    full_name = f"athena_validation.{validation['name']}"
    test_id = hashlib.md5(full_name.encode()).hexdigest()
    data = {
        "uuid": str(uuid.uuid4()),
        "historyId": test_id,
        "testCaseId": test_id,
        "fullName": full_name,
        "name": validation["name"],
        "status": status,
        "stage": "finished",
        "start": start,
        "stop": stop,
        "labels": [
            {"name": "framework", "value": "great_expectations"},
            {"name": "suite", "value": "athena"},
            {"name": "feature", "value": "Athena data validation"},
        ],
        "parameters": [{"name": "table", "value": str(env(validation.get("table", "")))}],
    }
    if message:
        data["statusDetails"] = {"message": message, "trace": trace or message}
    (ALLURE_RESULTS / f"{data['uuid']}-result.json").write_text(json.dumps(data, indent=2) + "\n")


def run_validation(context, validation, checkpoint):
    source = checkpoint["sources"][validation["source"]]
    start = int(time.time() * 1000)
    try:
        result = gx_validate(context, validation, athena_frame(source, validation), checkpoint.get("rules", []))
    except Exception as exc:
        allure_result(validation, "broken", start, int(time.time() * 1000), str(exc), repr(exc))
        raise
    allure_result(
        validation,
        "passed" if result.success else "failed",
        start,
        int(time.time() * 1000),
        None if result.success else failure_message(result),
        None if result.success else json.dumps(result.describe(), indent=2, default=str),
    )
    return result


def build_allure_report():
    if shutil.which("allure"):
        cmd = ["allure", "generate", str(ALLURE_RESULTS), "--clean", "-o", str(ALLURE_REPORT)]
    elif shutil.which("npx"):
        cmd = ["npx", "--yes", "allure-commandline", "generate", str(ALLURE_RESULTS), "--clean", "-o", str(ALLURE_REPORT)]
    else:
        print("Allure HTML skipped: install allure or npx.")
        return False
    subprocess.run(cmd, check=True)
    print(f"file://{ALLURE_REPORT / 'index.html'}")
    return True


def main(paths):
    load_env()
    reset_outputs()
    context = gx_context()
    success = True
    for path in paths or [DEFAULT_CHECKPOINT]:
        checkpoint = load_yaml(path)
        for validation in checkpoint["validations"]:
            result = run_validation(context, validation, checkpoint)
            print(result.describe())
            success = success and result.success
    print(context.build_data_docs()["local_site"])
    build_allure_report()
    return 0 if success else 1


raise SystemExit(main(sys.argv[1:]))
