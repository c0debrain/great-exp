import os
import sys
import shutil
import warnings
from pathlib import Path

import great_expectations as gx
import yaml
from great_expectations import Checkpoint, ExpectationSuite
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.datasource.fluent import GxDatasourceWarning
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".gx_runtime"
REPORTS = ROOT / "reports"
CONFIG = ROOT / "config.yml"


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
        resolved = os.getenv(env_name)
        if not resolved:
            raise SystemExit(f"Environment variable '{env_name}' is required.")
        return resolved
    if isinstance(value, dict):
        return {key: resolve_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_env(item) for item in value]
    return value


load_config_env()


def load_expectation(rule_path):
    rule = load_yaml(rule_path)
    return ExpectationConfiguration(type=rule["expectation_type"], kwargs=rule["kwargs"], description=rule.get("description"), meta={"rule_name": rule["name"], "source_file": rule_path.name})


def build_suite(context, validation, default_rules):
    rules = validation.get("rules", default_rules)
    return context.suites.add_or_update(ExpectationSuite(name=f"{validation['name']}_suite", expectations=[load_expectation(ROOT / rule) for rule in rules]))


def build_csv_validation(context, validation, default_rules):
    suite = build_suite(context, validation, default_rules)
    asset = context.data_sources.add_pandas(name=f"{validation['name']}_ds").add_csv_asset(name=f"{validation['name']}_asset", filepath_or_buffer=ROOT / validation["file"])
    return context.validation_definitions.add_or_update(ValidationDefinition(name=validation["name"], data=asset.add_batch_definition_whole_dataframe("default_batch"), suite=suite))


def build_validation(context, validation, default_rules, _connections):
    if "file" in validation:
        return build_csv_validation(context, validation, default_rules)
    raise SystemExit(f"Unsupported validation config: {validation}")


def create_spark_session(spark_config):
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master(spark_config.get("master", "local[*]")).appName(
        spark_config.get("app_name", "gx-spark-validation")
    )
    for key, value in resolve_env(spark_config.get("config", {})).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_spark_dataframe(spark, source, validation):
    kind = source["kind"]
    if kind == "jdbc":
        return spark.read.jdbc(
            url=resolve_env(source["url"]),
            table=validation.get("table", source.get("table")),
            properties=resolve_env(source.get("properties", {})),
        )
    if kind == "table":
        return spark.table(validation.get("table", source["table"]))
    if kind == "sql":
        return spark.sql(validation.get("query", source["query"]))
    raise SystemExit(f"Unsupported Spark source kind: {kind}")


def run_spark_validation(context, spark, validation, default_rules, sources, spark_config):
    if validation["source"] not in sources:
        raise SystemExit(f"Source '{validation['source']}' is not defined in the checkpoint.")

    suite = build_suite(context, validation, default_rules)
    dataframe = read_spark_dataframe(spark, sources[validation["source"]], validation)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=GxDatasourceWarning)
        datasource = context.data_sources.add_spark(
            name=f"{validation['name']}_spark",
            spark_config={
                "spark.master": spark_config.get("master", "local[*]"),
                "spark.app.name": spark_config.get("app_name", "gx-spark-validation"),
            },
        )
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


def create_context():
    shutil.rmtree(RUNTIME, ignore_errors=True)
    shutil.rmtree(REPORTS, ignore_errors=True)
    RUNTIME.mkdir(exist_ok=True)
    REPORTS.mkdir(exist_ok=True)
    return gx.get_context(project_config=DataContextConfig(config_version=4.0, stores={"expectations_store": {"class_name": "ExpectationsStore", "store_backend": {"class_name": "TupleFilesystemStoreBackend", "base_directory": str(RUNTIME / "expectations")}}, "validation_results_store": {"class_name": "ValidationResultsStore", "store_backend": {"class_name": "TupleFilesystemStoreBackend", "base_directory": str(RUNTIME / "validations")}}, "checkpoint_store": {"class_name": "CheckpointStore", "store_backend": {"class_name": "TupleFilesystemStoreBackend", "suppress_store_backend_id": True, "base_directory": str(RUNTIME / "checkpoints")}}, "validation_definition_store": {"class_name": "ValidationDefinitionStore", "store_backend": {"class_name": "TupleFilesystemStoreBackend", "base_directory": str(RUNTIME / "validation_definitions")}}}, expectations_store_name="expectations_store", validation_results_store_name="validation_results_store", checkpoint_store_name="checkpoint_store", data_docs_sites={"local_site": {"class_name": "SiteBuilder", "show_how_to_buttons": False, "store_backend": {"class_name": "TupleFilesystemStoreBackend", "base_directory": str(REPORTS)}, "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"}}}, progress_bars={"globally": False, "metric_calculations": False}, analytics_enabled=False), mode="ephemeral")


def run_checkpoint(checkpoint_file):
    context = create_context()
    checkpoint = load_yaml(ROOT / checkpoint_file)
    validations = checkpoint["validations"]
    if any(validation.get("type") == "spark_table" for validation in validations):
        spark = create_spark_session(checkpoint.get("spark", {}))
        try:
            results = [
                run_spark_validation(
                    context,
                    spark,
                    validation,
                    checkpoint.get("rules", []),
                    checkpoint.get("sources", {}),
                    checkpoint.get("spark", {}),
                )
                for validation in validations
            ]
        finally:
            spark.stop()
        for result in results:
            print(result.describe())
        success = all(result.success for result in results)
    else:
        checkpoint = context.checkpoints.add_or_update(Checkpoint(name=checkpoint.get("name", Path(checkpoint_file).stem), validation_definitions=[build_validation(context, validation, checkpoint.get("rules", []), checkpoint.get("connections", {})) for validation in validations]))
        result = checkpoint.run()
        print(result.describe())
        success = result.success
    print(context.build_data_docs()["local_site"])
    return success


raise SystemExit(0 if all(run_checkpoint(path) for path in (sys.argv[1:] or ["checkpoints/customers.yml"])) else 1)
