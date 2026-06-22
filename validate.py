import os
import shutil
import sys
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
CONFIG = ROOT / "config.yml"
DEFAULT_CHECKPOINT = "checkpoints/athena_ministack.yml"


def load_yaml(path):
    return yaml.safe_load(path.read_text())


def load_config_env():
    if not CONFIG.exists():
        return
    for key, value in load_yaml(CONFIG).items():
        os.environ.setdefault(key, str(value))


def resolve_env(value):
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1]
        if env_name not in os.environ:
            raise SystemExit(f"Environment variable '{env_name}' is required.")
        return os.environ[env_name]
    if isinstance(value, dict):
        return {key: resolve_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_env(item) for item in value]
    return value


load_config_env()


def load_expectation(rule_path):
    rule = load_yaml(rule_path)
    return ExpectationConfiguration(
        type=rule["expectation_type"],
        kwargs=rule["kwargs"],
        description=rule.get("description"),
        meta={"rule_name": rule["name"], "source_file": rule_path.name},
    )


def build_suite(context, validation, default_rules):
    rules = validation.get("rules", default_rules)
    expectations = [load_expectation(ROOT / rule) for rule in rules]
    return context.suites.add_or_update(
        ExpectationSuite(name=f"{validation['name']}_suite", expectations=expectations)
    )


def create_athena_engine(source):
    region_name = resolve_env(source["region_name"])
    schema_name = resolve_env(source.get("schema_name", "default"))
    username = resolve_env(source.get("aws_access_key_id", ""))
    password = resolve_env(source.get("aws_secret_access_key", ""))
    query = {}
    for key in ("catalog_name", "work_group", "s3_staging_dir", "profile_name", "endpoint_url"):
        if key in source:
            query[key] = resolve_env(source[key])
    query.update(resolve_env(source.get("options", {})))
    url = URL.create(
        drivername=source.get("driver", "awsathena+rest"),
        username=username,
        password=password,
        host=f"athena.{region_name}.amazonaws.com",
        port=443,
        database=schema_name,
        query={key: str(value) for key, value in query.items()},
    )
    return create_engine(url)


def read_athena_dataframe(engine, source, validation):
    query = validation.get("query", source.get("query"))
    if query:
        sql = resolve_env(query)
    else:
        table = validation.get("table", source.get("table"))
        if not table:
            raise SystemExit(f"Athena validation requires either 'table' or 'query': {validation}")
        sql = f"SELECT * FROM {resolve_env(table)}"

    with engine.connect() as connection:
        return pd.read_sql_query(text(sql), connection)


def run_dataframe_validation(context, validation, dataframe, default_rules):
    suite = build_suite(context, validation, default_rules)
    datasource = context.data_sources.add_pandas(name=f"{validation['name']}_ds")
    asset = datasource.add_dataframe_asset(name=f"{validation['name']}_asset")
    validation_definition = context.validation_definitions.add_or_update(
        ValidationDefinition(
            name=validation["name"],
            data=asset.add_batch_definition_whole_dataframe("default_batch"),
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add_or_update(
        Checkpoint(name=validation["name"], validation_definitions=[validation_definition])
    )
    return checkpoint.run(batch_parameters={"dataframe": dataframe})


def run_athena_validation(context, validation, default_rules, sources):
    if validation["source"] not in sources:
        raise SystemExit(f"Source '{validation['source']}' is not defined in the checkpoint.")

    source = sources[validation["source"]]
    if source["kind"] != "athena_sqlalchemy":
        raise SystemExit(f"Unsupported Athena source kind: {source['kind']}")

    engine = create_athena_engine(source)
    try:
        dataframe = read_athena_dataframe(engine, source, validation)
        return run_dataframe_validation(context, validation, dataframe, default_rules)
    finally:
        engine.dispose()


def create_context():
    shutil.rmtree(RUNTIME, ignore_errors=True)
    shutil.rmtree(REPORTS, ignore_errors=True)
    RUNTIME.mkdir(exist_ok=True)
    REPORTS.mkdir(exist_ok=True)
    return gx.get_context(
        project_config=DataContextConfig(
            config_version=4.0,
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": str(RUNTIME / "expectations"),
                    },
                },
                "validation_results_store": {
                    "class_name": "ValidationResultsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": str(RUNTIME / "validations"),
                    },
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "suppress_store_backend_id": True,
                        "base_directory": str(RUNTIME / "checkpoints"),
                    },
                },
                "validation_definition_store": {
                    "class_name": "ValidationDefinitionStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": str(RUNTIME / "validation_definitions"),
                    },
                },
            },
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


def run_checkpoint(checkpoint_file):
    context = create_context()
    checkpoint = load_yaml(ROOT / checkpoint_file)
    results = [
        run_athena_validation(
            context,
            validation,
            checkpoint.get("rules", []),
            checkpoint.get("sources", {}),
        )
        for validation in checkpoint["validations"]
    ]
    for result in results:
        print(result.describe())
    print(context.build_data_docs()["local_site"])
    return all(result.success for result in results)


raise SystemExit(0 if all(run_checkpoint(path) for path in (sys.argv[1:] or [DEFAULT_CHECKPOINT])) else 1)
