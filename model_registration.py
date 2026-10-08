
# """
# model_registration.py
# ---------------------
# MLflow model registration to Unity Catalog with Champion/Challenger management.

# Run as a script:
#     python model_registration.py --pipeline total_cost --validate_only
#     python model_registration.py --pipeline total_cost
#     python model_registration.py --pipeline delay_days --skip_baseline

# Run inside a notebook (no sys.exit, real tracebacks):
#     import mlflow
#     from src.config.registry_config import get_pipeline
#     from src.model_registration.model_registration import run_pipeline

#     mlflow.set_registry_uri("databricks-uc")
#     pipeline = get_pipeline("total_cost")
#     run_pipeline(spark, pipeline, fail_fast=True)

# Everything table- or column-specific lives in src/config/registry_config.py.

# Promotion logic:
#     1. Nothing registered       -> new model becomes Champion
#     2. New model is better      -> new = Champion, old = Challenger
#     3. New model is not better  -> new = Challenger, Champion stays
# """
# from __future__ import annotations

# import argparse
# import logging
# import sys
# from datetime import datetime
# from typing import Any, Dict, List, Optional, TypedDict

# import numpy as np
# import pandas as pd
# from dateutil.relativedelta import relativedelta
# from pyspark.sql import Column, DataFrame, SparkSession, functions as F

# import mlflow
# from mlflow.exceptions import MlflowException
# from mlflow.tracking import MlflowClient

# from registry_config import (
#     DEFAULT_PIPELINE,
#     PIPELINES,
#     ModelSpec,
#     PipelineConfig,
#     TableConfig,
#     get_pipeline,
# )
# from src.model_registration.model_formats import load_model_file, log_model_object
# from src.model_registration.promotion_functions import (
#     _get_champion_version,
#     _is_model_better,
#     _print_final_state,
#     _promote_to_champion,
#     _register_as_challenger,
#     _register_as_champion,
#     _safe_get_run,
# )
# from src.utils.retry import _retry

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# logger = logging.getLogger(__name__)

# # Name of the DATE column this module adds to the feature DataFrame.
# EVENT_DATE_COL = "__event_date"


# class RegistrationResult(TypedDict):
#     registered_model_name: str
#     new_version: str
#     new_alias: str           # "champion" | "challenger"
#     champion_version: str
#     run_id: str
#     metric: str
#     metric_value: float
#     scenario: str            # first_registration | promoted | challenger | skipped_duplicate


# # ──────────────────────────────────────────────────────────────────────────
# # Date resolution + schema guards
# # ──────────────────────────────────────────────────────────────────────────
# def _date_from_parts(year: str, month: str, day: str) -> Column:
#     """
#     Compose a DATE from three numeric columns without ever raising.

#     Why this is not simply make_date(y, m, d):
#       * try_make_date does not exist on every DBR version (UNRESOLVED_ROUTINE)
#       * make_date RAISES on impossible dates (e.g. Feb 30) when ANSI mode is on,
#         and wrapping it in when(...) does NOT help — Spark evaluates both
#         branches, so the exception fires anyway

#     Strategy that is safe on every runtime and ANSI setting:
#       1. clamp year/month into always-valid ranges, so the date call cannot fail
#       2. build the FIRST of that month — always a real date
#       3. add (day - 1) days with date_add — arithmetic, never throws
#       4. if the month changed, the day overflowed (Feb 30 -> Mar 1) -> NULL
#       5. NULL out anything implausible from the start
#     """
#     y = F.col(year).cast("int")
#     m = F.col(month).cast("int")
#     d = F.col(day).cast("int")

#     plausible = (
#         y.isNotNull() & m.isNotNull() & d.isNotNull()
#         & y.between(1000, 9999) & m.between(1, 12) & d.between(1, 31)
#     )

#     # Clamped inputs — guarantee the construction itself cannot raise.
#     y_safe = F.when(y.between(1000, 9999), y).otherwise(F.lit(2000))
#     m_safe = F.when(m.between(1, 12), m).otherwise(F.lit(1))
#     d_safe = F.when(d.between(1, 31), d).otherwise(F.lit(1))

#     # First of month from a zero-padded string: always a valid date, so
#     # to_date never has to fail, and no make_date/try_make_date is needed.
#     first_of_month = F.to_date(
#         F.concat(
#             F.lpad(y_safe.cast("string"), 4, "0"),
#             F.lpad(m_safe.cast("string"), 2, "0"),
#             F.lit("01"),
#         ),
#         "yyyyMMdd",
#     )

#     candidate = F.date_add(first_of_month, d_safe - F.lit(1))

#     # Month drift means the day overflowed that month's length.
#     in_month = F.month(candidate) == m_safe

#     return F.when(plausible & in_month, candidate).otherwise(F.lit(None).cast("date"))


# def to_date_col(cfg: TableConfig) -> Column:
#     """
#     Build a DATE Column from the feature table according to cfg.date_kind.

#     "parts" exists because tables like sample_data_prepared store the date as
#     three separate INT columns (REQ_YEAR / REQ_MONTH / REQ_DAY) and there is
#     no single column to cast.
#     """
#     kind = cfg.date_kind

#     if kind == "parts":
#         return _date_from_parts(*cfg.date_parts)

#     if kind == "yyyymmdd_int":                   # 20240131
#         return F.to_date(F.col(cfg.date_col).cast("string"), "yyyyMMdd")

#     if kind == "unix_days":                      # days since 1970-01-01
#         return F.date_from_unix_date(F.col(cfg.date_col).cast("int"))

#     if kind == "unix_seconds":                   # epoch seconds
#         return F.to_date(F.from_unixtime(F.col(cfg.date_col).cast("long")))

#     if cfg.date_format:                          # string column
#         return F.to_date(F.col(cfg.date_col).cast("string"), cfg.date_format)

#     return F.col(cfg.date_col).cast("date")      # already DATE/TIMESTAMP


# def date_source_columns(cfg: TableConfig) -> List[str]:
#     """Which raw columns feed the event date."""
#     return list(cfg.date_parts) if cfg.date_kind == "parts" else [cfg.date_col]


# def with_event_date(df: DataFrame, cfg: TableConfig) -> DataFrame:
#     """Attach the resolved DATE column and drop sentinel rows."""
#     out = df.withColumn(EVENT_DATE_COL, to_date_col(cfg))
#     if cfg.min_valid_date:
#         out = out.filter(F.col(EVENT_DATE_COL) >= F.to_date(F.lit(cfg.min_valid_date)))
#     return out


# def require_columns(df: DataFrame, columns: List[str], table_name: str) -> None:
#     """Readable failure naming the table and its actual columns."""
#     available = set(df.columns)
#     missing = [c for c in columns if c not in available]
#     if missing:
#         raise ValueError(
#             f"Table '{table_name}' is missing column(s) {missing}.\n"
#             f"Available columns: {sorted(available)}\n"
#             f"Fix the names in registry_config.py."
#         )


# def validate_table_config(spark: SparkSession, cfg: TableConfig) -> None:
#     """Check the column contract before any MLflow work happens."""
#     cfg.validate()

#     features = spark.read.table(cfg.feature_table)
#     require_columns(features, date_source_columns(cfg), cfg.feature_table)
#     if cfg.join_keys:
#         require_columns(features, cfg.join_keys, cfg.feature_table)
#     for col in cfg.exclude_from_features:
#         if col not in features.columns:
#             logger.warning(
#                 "exclude_from_features lists '%s', absent from '%s' — ignoring.",
#                 col, cfg.feature_table,
#             )

#     if cfg.actual_table and spark.catalog.tableExists(cfg.actual_table):
#         actuals = spark.read.table(cfg.actual_table)
#         require_columns(
#             actuals, cfg.resolved_join_keys() + [cfg.label_col], cfg.actual_table
#         )

#     # Did the date actually resolve?
#     usable = (
#         features.select(to_date_col(cfg).alias("d"))
#         .filter(F.col("d").isNotNull())
#         .limit(1)
#         .count()
#     )
#     if usable == 0:
#         dtypes = dict(features.dtypes)
#         detail = {c: dtypes.get(c) for c in date_source_columns(cfg)}
#         raise ValueError(
#             f"Could not build a DATE from '{cfg.feature_table}' with "
#             f"date_kind='{cfg.date_kind}' and columns {detail} — all NULL.\n"
#             f"  separate year/month/day INTs -> date_kind='parts'\n"
#             f"  INT like 20240131            -> date_kind='yyyymmdd_int'\n"
#             f"  INT days since epoch         -> date_kind='unix_days'\n"
#             f"  STRING                       -> date_kind='auto' + date_format"
#         )

#     if cfg.min_valid_date:
#         total = features.count()
#         kept = with_event_date(features, cfg).count()
#         logger.info(
#             "Date coverage: %d / %d rows on or after %s (%d dropped as sentinel/NULL).",
#             kept, total, cfg.min_valid_date, total - kept,
#         )
#         if kept == 0:
#             raise ValueError(
#                 f"Every row in '{cfg.feature_table}' falls before "
#                 f"min_valid_date={cfg.min_valid_date}. Lower or remove it."
#             )

#     logger.info("Config validated against %s", cfg.feature_table)


# # ──────────────────────────────────────────────────────────────────────────
# # Retry-wrapped MLflow calls
# # ──────────────────────────────────────────────────────────────────────────
# @_retry()
# def _safe_search_runs(client: MlflowClient, experiment_ids: List[str],
#                       order_by: List[str], max_results: int):
#     return client.search_runs(
#         experiment_ids=experiment_ids, order_by=order_by, max_results=max_results
#     )


# @_retry()
# def _safe_search_model_versions(client: MlflowClient, filter_string: str):
#     return client.search_model_versions(filter_string)


# def _run_already_registered(existing_versions, run_id: str, model_name: str) -> bool:
#     for v in existing_versions:
#         if v.run_id == run_id:
#             logger.warning(
#                 "Run '%s' is already version %s of '%s'. Skipping.",
#                 run_id, v.version, model_name,
#             )
#             return True
#     return False


# def _build_model_name(catalog: str, schema: str, model_name: str) -> str:
#     return f"{catalog}.{schema}.{model_name}"


# # ──────────────────────────────────────────────────────────────────────────
# # Resolve a ModelSpec to a run
# # ──────────────────────────────────────────────────────────────────────────
# def _log_file_as_run(spec: ModelSpec, input_example: Optional[pd.DataFrame] = None) -> str:
#     """Load a .pkl/.ubj/.cb/... file, log it under the right flavor, return run_id."""
#     model, fmt = load_model_file(spec.model_path, spec.model_format, spec.loader_kwargs)

#     if spec.experiment_path:
#         mlflow.set_experiment(spec.experiment_path)

#     run_name = f"{spec.model_name}_{fmt.name}_{datetime.utcnow():%Y%m%d_%H%M%S}"
#     with mlflow.start_run(run_name=run_name) as run:
#         mlflow.set_tags({
#             "model_type": fmt.name,
#             "source_artifact": spec.model_path,
#             "registered_via": "file",
#             **spec.tags,
#         })
#         if spec.params:
#             mlflow.log_params(spec.params)
#         mlflow.log_metrics(spec.metrics)

#         log_model_object(
#             model,
#             fmt,
#             artifact_path="model",
#             input_example=input_example,
#             extra_pip_requirements=fmt.pip_requirements,
#         )
#         logger.info("Logged %s model from %s as run %s",
#                     fmt.name, spec.model_path, run.info.run_id)
#         return run.info.run_id


# def _find_experiment(path: str):
#     """
#     Look up an experiment, tolerating a trailing slash.

#     MLflow matches experiment names exactly, so
#     '/Workspace/Users/me/sample_model/' and '.../sample_model' are different
#     strings and only one of them exists.
#     """
#     candidates = [path, path.rstrip("/")]
#     if not path.endswith("/"):
#         candidates.append(path + "/")

#     for candidate in dict.fromkeys(candidates):          # preserve order, dedupe
#         experiment = mlflow.get_experiment_by_name(candidate)
#         if experiment is not None:
#             if candidate != path:
#                 logger.info("Matched experiment as '%s'.", candidate)
#             return experiment

#     raise ValueError(
#         f"Experiment '{path}' not found (also tried with/without a trailing "
#         f"slash). Use the workspace path (/Workspace/Users/...), not the "
#         f"browser URL. List what exists with:\n"
#         f"    mlflow.search_experiments()"
#     )


# def _resolve_source_run(client: MlflowClient, spec: ModelSpec,
#                         input_example: Optional[pd.DataFrame] = None):
#     if spec.source == "run":
#         logger.info("Source: run_id %s", spec.run_id)
#         return _safe_get_run(client, spec.run_id), spec.run_id

#     if spec.source == "file":
#         logger.info("Source: file %s", spec.model_path)
#         run_id = _log_file_as_run(spec, input_example=input_example)
#         return _safe_get_run(client, run_id), run_id

#     logger.info("Source: best run in %s", spec.experiment_path)
#     experiment = _find_experiment(spec.experiment_path)

#     order = "ASC" if spec.metric_direction == "minimize" else "DESC"
#     runs = _safe_search_runs(
#         client,
#         experiment_ids=[experiment.experiment_id],
#         order_by=[f"metrics.{spec.metric} {order}"],
#         max_results=1,
#     )
#     if not runs:
#         raise ValueError(f"No runs found in experiment '{spec.experiment_path}'.")
#     return runs[0], runs[0].info.run_id


# # ──────────────────────────────────────────────────────────────────────────
# # Core registration
# # ──────────────────────────────────────────────────────────────────────────
# def model_registration(
#     spec: ModelSpec,
#     catalog: str,
#     schema: str,
#     input_example: Optional[pd.DataFrame] = None,
# ) -> RegistrationResult:
#     """Register one ModelSpec with Champion/Challenger handling."""
#     spec.validate()

#     client = MlflowClient()
#     registered_model_name = _build_model_name(catalog, schema, spec.model_name)

#     logger.info("Target model : %s", registered_model_name)
#     logger.info("Metric       : %s (%s)", spec.metric, spec.metric_direction)

#     new_run, new_run_id = _resolve_source_run(client, spec, input_example)

#     new_metric_value = new_run.data.metrics.get(spec.metric)
#     if new_metric_value is None:
#         raise ValueError(
#             f"Metric '{spec.metric}' not found in run '{new_run_id}'. "
#             f"Available: {sorted(new_run.data.metrics)}"
#         )

#     logger.info(
#         "New model — run_id: %s | run_name: %s | model_type: %s | %s: %.4f",
#         new_run_id,
#         new_run.data.tags.get("mlflow.runName", "N/A"),
#         new_run.data.tags.get("model_type", "N/A"),
#         spec.metric,
#         new_metric_value,
#     )

#     try:
#         existing_versions = _safe_search_model_versions(
#             client, f"name='{registered_model_name}'"
#         )
#         model_exists = len(existing_versions) > 0
#     except MlflowException as exc:
#         if "RESOURCE_DOES_NOT_EXIST" in str(exc):
#             logger.info("Model not yet registered in Unity Catalog.")
#             model_exists, existing_versions = False, []
#         else:
#             raise

#     if model_exists and _run_already_registered(
#         existing_versions, new_run_id, registered_model_name
#     ):
#         return RegistrationResult(
#             registered_model_name=registered_model_name,
#             new_version="N/A",
#             new_alias="already_registered",
#             champion_version="N/A",
#             run_id=new_run_id,
#             metric=spec.metric,
#             metric_value=new_metric_value,
#             scenario="skipped_duplicate",
#         )

#     champion_version = (
#         _get_champion_version(client, registered_model_name, existing_versions)
#         if model_exists else None
#     )

#     if champion_version is None:
#         if model_exists:
#             logger.warning("Versions exist but no Champion alias — registering as Champion.")
#         else:
#             logger.info("SCENARIO: first registration — registering as Champion.")
#         result = _register_as_champion(
#             client=client,
#             run_id=new_run_id,
#             registered_model_name=registered_model_name,
#             new_run=new_run,
#             new_metric_value=new_metric_value,
#             metric=spec.metric,
#         )
#     else:
#         champion_run = _safe_get_run(client, champion_version.run_id)
#         champion_metric_value = champion_run.data.metrics.get(spec.metric)
#         if champion_metric_value is None:
#             raise ValueError(
#                 f"Champion version {champion_version.version} has no metric "
#                 f"'{spec.metric}' — cannot compare."
#             )

#         logger.info("Current Champion — version: %s | %s: %.4f",
#                     champion_version.version, spec.metric, champion_metric_value)

#         if _is_model_better(
#             new_value=new_metric_value,
#             champion_value=champion_metric_value,
#             metric_direction=spec.metric_direction,
#             improvement_threshold=spec.improvement_threshold,
#         ):
#             logger.info("SCENARIO: new model wins — promoting; old Champion -> Challenger.")
#             result = _promote_to_champion(
#                 client=client,
#                 run_id=new_run_id,
#                 registered_model_name=registered_model_name,
#                 new_run=new_run,
#                 new_metric_value=new_metric_value,
#                 metric=spec.metric,
#                 old_champion_version=champion_version.version,
#             )
#         else:
#             logger.info("SCENARIO: Champion holds — registering new model as Challenger.")
#             result = _register_as_challenger(
#                 client=client,
#                 run_id=new_run_id,
#                 registered_model_name=registered_model_name,
#                 new_run=new_run,
#                 new_metric_value=new_metric_value,
#                 metric=spec.metric,
#                 champion_version=champion_version.version,
#             )

#     _print_final_state(client, registered_model_name, spec.metric)
#     result["registered_model_name"] = registered_model_name
#     result["metric"] = spec.metric
#     return result


# def run_all_registrations(
#     models: Dict[str, ModelSpec],
#     catalog: str,
#     schema: str,
#     input_example: Optional[pd.DataFrame] = None,
#     fail_fast: bool = False,
# ) -> Dict[str, Any]:
#     """
#     Register every spec. Returns key -> RegistrationResult | {"error": ...}.

#     Errors are captured so one bad model does not abort the rest. Pass
#     fail_fast=True (or --fail_fast) to let the exception propagate instead —
#     useful in a notebook, where a caught error only shows up as SystemExit: 1.
#     """
#     results: Dict[str, Any] = {}
#     for model_key, spec in models.items():
#         logger.info("=" * 70)
#         logger.info("Processing model key: %s", model_key)
#         try:
#             results[model_key] = model_registration(
#                 spec, catalog=catalog, schema=schema, input_example=input_example
#             )
#         except Exception as exc:
#             if fail_fast:
#                 raise
#             logger.error("Registration failed for '%s': %s", model_key, exc, exc_info=True)
#             results[model_key] = {"error": str(exc)}
#     return results


# # ──────────────────────────────────────────────────────────────────────────
# # Test window + baseline
# # ──────────────────────────────────────────────────────────────────────────
# def get_test_ranges(spark: SparkSession, cfg: TableConfig) -> tuple[str, str]:
#     """test_end = MAX(event_date); test_start = test_end - cfg.test_months."""
#     features = spark.read.table(cfg.feature_table)
#     require_columns(features, date_source_columns(cfg), cfg.feature_table)

#     raw = (
#         with_event_date(features, cfg)
#         .select(F.max(F.col(EVENT_DATE_COL)).alias("max_date"))
#         .collect()[0]["max_date"]
#     )
#     if raw is None:
#         raise ValueError(
#             f"No usable dates in '{cfg.feature_table}' with date_kind="
#             f"'{cfg.date_kind}' on {date_source_columns(cfg)}. "
#             f"Table may be empty or every date is a sentinel."
#         )

#     start = (raw - relativedelta(months=cfg.test_months)).strftime("%Y-%m-%d")
#     end = raw.strftime("%Y-%m-%d")
#     logger.info("Test window — %s -> %s (%d months)", start, end, cfg.test_months)
#     return start, end


# def build_predict_input(features_pd: pd.DataFrame, cfg: TableConfig) -> pd.DataFrame:
#     """Drop identifiers, helper columns and labels before model.predict()."""
#     drop = set(cfg.exclude_from_features) | {EVENT_DATE_COL, cfg.label_col}
#     return features_pd.drop(columns=[c for c in drop if c in features_pd.columns])


# def create_baseline_after_registration(
#     spark: SparkSession,
#     registration_result: dict,
#     cfg: TableConfig,
#     test_start_date: str,
#     test_end_date: str,
# ) -> None:
#     """Score the test window with the Champion, join actuals, overwrite the baseline."""
#     if registration_result.get("scenario") == "skipped_duplicate":
#         logger.info("Skipping baseline creation — run already registered.")
#         return

#     model_name = registration_result["registered_model_name"]
#     client = MlflowClient()
#     mv = client.get_model_version_by_alias(model_name, "champion")
#     model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

#     logger.info("Baseline — model: %s | version: %s | window: %s -> %s",
#                 model_name, mv.version, test_start_date, test_end_date)

#     features_df = with_event_date(spark.read.table(cfg.feature_table), cfg).filter(
#         (F.col(EVENT_DATE_COL) >= F.to_date(F.lit(test_start_date)))
#         & (F.col(EVENT_DATE_COL) <= F.to_date(F.lit(test_end_date)))
#     )

#     row_count = features_df.count()
#     if row_count == 0:
#         raise ValueError(
#             f"No rows in '{cfg.feature_table}' between {test_start_date} and "
#             f"{test_end_date}. Check date_kind and test_months."
#         )
#     logger.info("Feature rows in test window: %d", row_count)

#     features_pd = features_df.toPandas()
#     raw_preds = model.predict(build_predict_input(features_pd, cfg))

#     if isinstance(raw_preds, pd.DataFrame):
#         pred_values = (
#             raw_preds[cfg.prediction_col].values
#             if cfg.prediction_col in raw_preds.columns
#             else raw_preds.iloc[:, 0].values
#         )
#     else:
#         pred_values = np.asarray(raw_preds).ravel()

#     if len(pred_values) != len(features_pd):
#         raise ValueError(
#             f"Model returned {len(pred_values)} predictions for {len(features_pd)} rows."
#         )

#     features_pd["event_date"] = pd.to_datetime(features_pd[EVENT_DATE_COL])
#     features_pd = features_pd.drop(columns=[EVENT_DATE_COL])
#     features_pd["prediction_final"] = pred_values.astype(np.float32)
#     features_pd["model_version"] = str(mv.version)
#     features_pd["model_name"] = model_name
#     features_pd["prediction_timestamp"] = datetime.utcnow()

#     if cfg.actual_table and spark.catalog.tableExists(cfg.actual_table):
#         join_keys = cfg.resolved_join_keys()
#         actuals_sdf = spark.read.table(cfg.actual_table)
#         require_columns(actuals_sdf, join_keys + [cfg.label_col], cfg.actual_table)

#         total = actuals_sdf.count()
#         distinct = actuals_sdf.select(*join_keys).distinct().count()
#         if total != distinct:
#             logger.warning(
#                 "'%s' has %d rows but only %d distinct %s — the join will fan out "
#                 "and inflate the baseline. Add columns to join_keys.",
#                 cfg.actual_table, total, distinct, join_keys,
#             )

#         actuals_pd = actuals_sdf.select(*join_keys, cfg.label_col).toPandas()
#         before = len(features_pd)
#         features_pd = features_pd.merge(actuals_pd, on=join_keys, how="left")
#         if len(features_pd) != before:
#             logger.warning("Join changed row count: %d -> %d.", before, len(features_pd))

#         features_pd[cfg.label_col] = features_pd[cfg.label_col].astype(np.float32)
#         matched = int(features_pd[cfg.label_col].notna().sum())
#         logger.info("Actuals joined: %d / %d rows have '%s'",
#                     matched, len(features_pd), cfg.label_col)
#         if matched == 0:
#             logger.warning(
#                 "Join on %s matched nothing — check key dtypes on both sides "
#                 "(a string/bigint mismatch joins to zero rows silently).", join_keys,
#             )
#     else:
#         logger.warning("Actuals table '%s' not found — labels will be NULL.", cfg.actual_table)
#         features_pd[cfg.label_col] = np.full(len(features_pd), np.nan, dtype=np.float32)

#     (
#         spark.createDataFrame(features_pd)
#         .write.format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(cfg.baseline_table)
#     )
#     logger.info("Baseline written: '%s' | %d rows | model version %s",
#                 cfg.baseline_table, len(features_pd), mv.version)


# # ──────────────────────────────────────────────────────────────────────────
# # Entrypoint
# # ──────────────────────────────────────────────────────────────────────────
# def parse_args():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--pipeline", default=DEFAULT_PIPELINE,
#                         choices=sorted(PIPELINES), help="Which PIPELINES entry to run.")
#     parser.add_argument("--catalog", default=None, help="Override pipeline catalog.")
#     parser.add_argument("--schema", default=None, help="Override pipeline schema.")
#     parser.add_argument("--experiment_path", default=None,
#                         help="Override experiment_path on experiment-sourced models.")
#     parser.add_argument("--test_months", type=int, default=None)
#     parser.add_argument("--skip_baseline", action="store_true")
#     parser.add_argument("--validate_only", action="store_true",
#                         help="Check the column contract and exit.")
#     parser.add_argument("--fail_fast", action="store_true",
#                         help="Raise the first exception instead of capturing it. "
#                              "Use in notebooks, where a captured error shows "
#                              "only as SystemExit: 1.")
#     args, _ = parser.parse_known_args()
#     return args


# def run_pipeline(spark: SparkSession, pipeline: PipelineConfig,
#                  skip_baseline: bool = False, fail_fast: bool = False) -> int:
#     """Validate, register every model, refresh baselines. Returns a POSIX exit code."""
#     cfg = pipeline.tables
#     validate_table_config(spark, cfg)
#     test_start_date, test_end_date = get_test_ranges(spark, cfg)

#     input_example = None
#     if any(s.source == "file" for s in pipeline.models.values()):
#         sample = with_event_date(spark.read.table(cfg.feature_table), cfg).limit(5).toPandas()
#         input_example = build_predict_input(sample, cfg)

#     results = run_all_registrations(
#         pipeline.models,
#         catalog=pipeline.catalog,
#         schema=pipeline.schema,
#         input_example=input_example,
#         fail_fast=fail_fast,
#     )

#     failures: List[str] = []
#     for key, res in results.items():
#         logger.info("Result for %s: %s", key, res)
#         if not isinstance(res, dict) or "error" in res:
#             failures.append(f"{key}: registration — {res.get('error', 'unknown')}")
#             continue
#         if skip_baseline or not pipeline.models[key].create_baseline:
#             continue
#         try:
#             create_baseline_after_registration(
#                 spark=spark,
#                 registration_result=res,
#                 cfg=cfg,
#                 test_start_date=test_start_date,
#                 test_end_date=test_end_date,
#             )
#         except Exception as exc:
#             if fail_fast:
#                 raise
#             failures.append(f"{key}: baseline — {exc}")
#             logger.error("Baseline creation failed for '%s': %s", key, exc, exc_info=True)

#     # Spell out why the exit code is non-zero — in a notebook the caller only
#     # sees "SystemExit: 1" otherwise.
#     logger.info("=" * 70)
#     if failures:
#         logger.error("Pipeline '%s' finished with %d failure(s):", pipeline.name, len(failures))
#         for line in failures:
#             logger.error("  - %s", line)
#         logger.error("Re-run with fail_fast=True to get the full traceback.")
#         return 1

#     logger.info("Pipeline '%s' completed successfully.", pipeline.name)
#     return 0


# def main() -> int:
#     args = parse_args()
#     spark = SparkSession.builder.getOrCreate()
#     mlflow.set_registry_uri("databricks-uc")

#     pipeline = get_pipeline(args.pipeline)
#     logger.info("Pipeline: %s", pipeline.name)

#     if args.catalog:
#         pipeline.catalog = args.catalog
#     if args.schema:
#         pipeline.schema = args.schema
#     if args.test_months is not None:
#         pipeline.tables.test_months = args.test_months
#     if args.experiment_path:
#         pipeline.experiment_path = args.experiment_path
#         for spec in pipeline.models.values():
#             if spec.source == "experiment":
#                 spec.experiment_path = args.experiment_path

#     if args.validate_only:
#         validate_table_config(spark, pipeline.tables)
#         start, end = get_test_ranges(spark, pipeline.tables)
#         logger.info("Validation passed. Test window: %s -> %s", start, end)
#         return 0

#     return run_pipeline(
#         spark,
#         pipeline,
#         skip_baseline=args.skip_baseline,
#         fail_fast=args.fail_fast,
#     )


# if __name__ == "__main__":
#     main()









"""
model_registration.py
---------------------
MLflow model registration to Unity Catalog with Champion/Challenger management.

Run as a script:
    python model_registration.py --pipeline total_cost --validate_only
    python model_registration.py --pipeline total_cost
    python model_registration.py --pipeline delay_days --skip_baseline

Run inside a notebook (no sys.exit, real tracebacks):
    import mlflow
    from src.config.registry_config import get_pipeline
    from src.model_registration.model_registration import run_pipeline

    mlflow.set_registry_uri("databricks-uc")
    pipeline = get_pipeline("total_cost")
    run_pipeline(spark, pipeline, fail_fast=True)

Everything table- or column-specific lives in src/config/registry_config.py.

Promotion logic:
    1. Nothing registered       -> new model becomes Champion
    2. New model is better      -> new = Champion, old = Challenger
    3. New model is not better  -> new = Challenger, Champion stays
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark.sql import Column, DataFrame, SparkSession, functions as F

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from registry_config import (
    DEFAULT_PIPELINE,
    PIPELINES,
    ModelSpec,
    PipelineConfig,
    TableConfig,
    get_pipeline,
)
from src.model_registration.model_formats import load_model_file, log_model_object
from src.model_registration.promotion_functions import (
    _get_champion_version,
    _is_model_better,
    _print_final_state,
    _promote_to_champion,
    _register_as_challenger,
    _register_as_champion,
    _safe_get_run,
)
from src.utils.retry import _retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Name of the DATE column this module adds to the feature DataFrame.
EVENT_DATE_COL = "__event_date"


class RegistrationResult(TypedDict):
    registered_model_name: str
    new_version: str
    new_alias: str           # "champion" | "challenger"
    champion_version: str
    run_id: str
    metric: str
    metric_value: float
    scenario: str            # first_registration | promoted | challenger | skipped_duplicate


# ──────────────────────────────────────────────────────────────────────────
# Date resolution + schema guards
# ──────────────────────────────────────────────────────────────────────────
def _date_from_parts(year: str, month: str, day: str) -> Column:
    """
    Compose a DATE from three numeric columns without ever raising.

    Why this is not simply make_date(y, m, d):
      * try_make_date does not exist on every DBR version (UNRESOLVED_ROUTINE)
      * make_date RAISES on impossible dates (e.g. Feb 30) when ANSI mode is on,
        and wrapping it in when(...) does NOT help — Spark evaluates both
        branches, so the exception fires anyway

    Strategy that is safe on every runtime and ANSI setting:
      1. clamp year/month into always-valid ranges, so the date call cannot fail
      2. build the FIRST of that month — always a real date
      3. add (day - 1) days with date_add — arithmetic, never throws
      4. if the month changed, the day overflowed (Feb 30 -> Mar 1) -> NULL
      5. NULL out anything implausible from the start
    """
    y = F.col(year).cast("int")
    m = F.col(month).cast("int")
    d = F.col(day).cast("int")

    plausible = (
        y.isNotNull() & m.isNotNull() & d.isNotNull()
        & y.between(1000, 9999) & m.between(1, 12) & d.between(1, 31)
    )

    # Clamped inputs — guarantee the construction itself cannot raise.
    y_safe = F.when(y.between(1000, 9999), y).otherwise(F.lit(2000))
    m_safe = F.when(m.between(1, 12), m).otherwise(F.lit(1))
    d_safe = F.when(d.between(1, 31), d).otherwise(F.lit(1))

    # First of month from a zero-padded string: always a valid date, so
    # to_date never has to fail, and no make_date/try_make_date is needed.
    first_of_month = F.to_date(
        F.concat(
            F.lpad(y_safe.cast("string"), 4, "0"),
            F.lpad(m_safe.cast("string"), 2, "0"),
            F.lit("01"),
        ),
        "yyyyMMdd",
    )

    candidate = F.date_add(first_of_month, d_safe - F.lit(1))

    # Month drift means the day overflowed that month's length.
    in_month = F.month(candidate) == m_safe

    return F.when(plausible & in_month, candidate).otherwise(F.lit(None).cast("date"))


def to_date_col(cfg: TableConfig) -> Column:
    """
    Build a DATE Column from the feature table according to cfg.date_kind.

    "parts" exists because tables like sample_data_prepared store the date as
    three separate INT columns (REQ_YEAR / REQ_MONTH / REQ_DAY) and there is
    no single column to cast.
    """
    kind = cfg.date_kind

    if kind == "parts":
        return _date_from_parts(*cfg.date_parts)

    if kind == "yyyymmdd_int":                   # 20240131
        return F.to_date(F.col(cfg.date_col).cast("string"), "yyyyMMdd")

    if kind == "unix_days":                      # days since 1970-01-01
        return F.date_from_unix_date(F.col(cfg.date_col).cast("int"))

    if kind == "unix_seconds":                   # epoch seconds
        return F.to_date(F.from_unixtime(F.col(cfg.date_col).cast("long")))

    if cfg.date_format:                          # string column
        return F.to_date(F.col(cfg.date_col).cast("string"), cfg.date_format)

    return F.col(cfg.date_col).cast("date")      # already DATE/TIMESTAMP


def date_source_columns(cfg: TableConfig) -> List[str]:
    """Which raw columns feed the event date."""
    return list(cfg.date_parts) if cfg.date_kind == "parts" else [cfg.date_col]


def with_event_date(df: DataFrame, cfg: TableConfig) -> DataFrame:
    """Attach the resolved DATE column and drop sentinel rows."""
    out = df.withColumn(EVENT_DATE_COL, to_date_col(cfg))
    if cfg.min_valid_date:
        out = out.filter(F.col(EVENT_DATE_COL) >= F.to_date(F.lit(cfg.min_valid_date)))
    return out


def require_columns(df: DataFrame, columns: List[str], table_name: str) -> None:
    """Readable failure naming the table and its actual columns."""
    available = set(df.columns)
    missing = [c for c in columns if c not in available]
    if missing:
        raise ValueError(
            f"Table '{table_name}' is missing column(s) {missing}.\n"
            f"Available columns: {sorted(available)}\n"
            f"Fix the names in registry_config.py."
        )


def validate_table_config(spark: SparkSession, cfg: TableConfig) -> None:
    """Check the column contract before any MLflow work happens."""
    cfg.validate()

    features = spark.read.table(cfg.feature_table)
    require_columns(features, date_source_columns(cfg), cfg.feature_table)
    if cfg.join_keys:
        require_columns(features, cfg.join_keys, cfg.feature_table)
    for col in cfg.exclude_from_features:
        if col not in features.columns:
            logger.warning(
                "exclude_from_features lists '%s', absent from '%s' — ignoring.",
                col, cfg.feature_table,
            )

    if cfg.actual_table and spark.catalog.tableExists(cfg.actual_table):
        actuals = spark.read.table(cfg.actual_table)
        require_columns(
            actuals, cfg.resolved_join_keys() + [cfg.label_col], cfg.actual_table
        )

    # Did the date actually resolve?
    usable = (
        features.select(to_date_col(cfg).alias("d"))
        .filter(F.col("d").isNotNull())
        .limit(1)
        .count()
    )
    if usable == 0:
        dtypes = dict(features.dtypes)
        detail = {c: dtypes.get(c) for c in date_source_columns(cfg)}
        raise ValueError(
            f"Could not build a DATE from '{cfg.feature_table}' with "
            f"date_kind='{cfg.date_kind}' and columns {detail} — all NULL.\n"
            f"  separate year/month/day INTs -> date_kind='parts'\n"
            f"  INT like 20240131            -> date_kind='yyyymmdd_int'\n"
            f"  INT days since epoch         -> date_kind='unix_days'\n"
            f"  STRING                       -> date_kind='auto' + date_format"
        )

    if cfg.min_valid_date:
        total = features.count()
        kept = with_event_date(features, cfg).count()
        logger.info(
            "Date coverage: %d / %d rows on or after %s (%d dropped as sentinel/NULL).",
            kept, total, cfg.min_valid_date, total - kept,
        )
        if kept == 0:
            raise ValueError(
                f"Every row in '{cfg.feature_table}' falls before "
                f"min_valid_date={cfg.min_valid_date}. Lower or remove it."
            )

    logger.info("Config validated against %s", cfg.feature_table)


# ──────────────────────────────────────────────────────────────────────────
# Retry-wrapped MLflow calls
# ──────────────────────────────────────────────────────────────────────────
@_retry()
def _safe_search_runs(client: MlflowClient, experiment_ids: List[str],
                      order_by: List[str], max_results: int):
    return client.search_runs(
        experiment_ids=experiment_ids, order_by=order_by, max_results=max_results
    )


@_retry()
def _safe_search_model_versions(client: MlflowClient, filter_string: str):
    return client.search_model_versions(filter_string)


def _run_already_registered(existing_versions, run_id: str, model_name: str) -> bool:
    for v in existing_versions:
        if v.run_id == run_id:
            logger.warning(
                "Run '%s' is already version %s of '%s'. Skipping.",
                run_id, v.version, model_name,
            )
            return True
    return False


def _build_model_name(catalog: str, schema: str, model_name: str) -> str:
    return f"{catalog}.{schema}.{model_name}"


# ──────────────────────────────────────────────────────────────────────────
# Resolve a ModelSpec to a run
# ──────────────────────────────────────────────────────────────────────────
def _log_file_as_run(spec: ModelSpec, input_example: Optional[pd.DataFrame] = None) -> str:
    """Load a .pkl/.ubj/.cb/... file, log it under the right flavor, return run_id."""
    model, fmt = load_model_file(spec.model_path, spec.model_format, spec.loader_kwargs)

    if spec.experiment_path:
        mlflow.set_experiment(spec.experiment_path)

    run_name = f"{spec.model_name}_{fmt.name}_{datetime.utcnow():%Y%m%d_%H%M%S}"
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.set_tags({
            "model_type": fmt.name,
            "source_artifact": spec.model_path,
            "registered_via": "file",
            **spec.tags,
        })
        if spec.params:
            mlflow.log_params(spec.params)
        mlflow.log_metrics(spec.metrics)

        log_model_object(
            model,
            fmt,
            artifact_path="model",
            input_example=input_example,
            extra_pip_requirements=fmt.pip_requirements,
        )
        logger.info("Logged %s model from %s as run %s",
                    fmt.name, spec.model_path, run.info.run_id)
        return run.info.run_id


def _resolve_metric(run, spec: ModelSpec) -> tuple[str, float]:
    """
    Find the metric to compare on in one run. Returns (key, value).

    Exact `metric` wins when present. Otherwise `metric_pattern` is matched
    against the run's metric keys — needed when keys carry a window stamp
    (rmse_val_2025-01-10_2025-03-07) and so differ run to run.
    """
    metrics = run.data.metrics

    if spec.metric and spec.metric in metrics:
        return spec.metric, metrics[spec.metric]

    if spec.metric_pattern:
        rx = re.compile(spec.metric_pattern)
        matches = sorted(k for k in metrics if rx.search(k))
        if matches:
            key = matches[-1] if spec.metric_select == "latest" else matches[0]
            if len(matches) > 1:
                logger.info(
                    "Pattern '%s' matched %d keys; using '%s' (%s).",
                    spec.metric_pattern, len(matches), key, spec.metric_select,
                )
            return key, metrics[key]

    wanted = spec.metric or f"pattern {spec.metric_pattern!r}"
    raise ValueError(
        f"No metric matching {wanted} in run '{run.info.run_id}'. "
        f"Available: {sorted(metrics)}"
    )


def _find_experiment(path: str):
    """
    Look up an experiment, tolerating a trailing slash.

    MLflow matches experiment names exactly, so
    '/Workspace/Users/me/sample_model/' and '.../sample_model' are different
    strings and only one of them exists.
    """
    candidates = [path, path.rstrip("/")]
    if not path.endswith("/"):
        candidates.append(path + "/")

    for candidate in dict.fromkeys(candidates):          # preserve order, dedupe
        experiment = mlflow.get_experiment_by_name(candidate)
        if experiment is not None:
            if candidate != path:
                logger.info("Matched experiment as '%s'.", candidate)
            return experiment

    raise ValueError(
        f"Experiment '{path}' not found (also tried with/without a trailing "
        f"slash). Use the workspace path (/Workspace/Users/...), not the "
        f"browser URL. List what exists with:\n"
        f"    mlflow.search_experiments()"
    )


def _resolve_source_run(client: MlflowClient, spec: ModelSpec,
                        input_example: Optional[pd.DataFrame] = None):
    if spec.source == "run":
        logger.info("Source: run_id %s", spec.run_id)
        return _safe_get_run(client, spec.run_id), spec.run_id

    if spec.source == "file":
        logger.info("Source: file %s", spec.model_path)
        run_id = _log_file_as_run(spec, input_example=input_example)
        return _safe_get_run(client, run_id), run_id

    logger.info("Source: best run in %s", spec.experiment_path)
    experiment = _find_experiment(spec.experiment_path)

    # An exact metric can be sorted server-side; a pattern cannot, because the
    # key differs per run. In that case pull a page of runs and rank here.
    if spec.metric and not spec.metric_pattern:
        order = "ASC" if spec.metric_direction == "minimize" else "DESC"
        runs = _safe_search_runs(
            client,
            experiment_ids=[experiment.experiment_id],
            order_by=[f"metrics.{spec.metric} {order}"],
            max_results=1,
        )
        if not runs:
            raise ValueError(f"No runs found in experiment '{spec.experiment_path}'.")
        return runs[0], runs[0].info.run_id

    runs = _safe_search_runs(
        client,
        experiment_ids=[experiment.experiment_id],
        order_by=["attributes.start_time DESC"],
        max_results=spec.search_max_results,
    )
    if not runs:
        raise ValueError(f"No runs found in experiment '{spec.experiment_path}'.")

    scored = []
    for run in runs:
        try:
            key, value = _resolve_metric(run, spec)
        except ValueError:
            continue                       # run predates the metric, or failed
        scored.append((value, key, run))

    if not scored:
        sample = sorted(runs[0].data.metrics) if runs else []
        raise ValueError(
            f"None of the {len(runs)} most recent runs in "
            f"'{spec.experiment_path}' have a metric matching "
            f"{spec.metric_pattern!r}. Metrics on the newest run: {sample}"
        )

    pick = min if spec.metric_direction == "minimize" else max
    value, key, best = pick(scored, key=lambda t: t[0])
    logger.info(
        "Ranked %d/%d runs on '%s'; best = %s (%s: %.4f)",
        len(scored), len(runs), spec.metric_pattern, best.info.run_id, key, value,
    )
    return best, best.info.run_id


# ──────────────────────────────────────────────────────────────────────────
# Core registration
# ──────────────────────────────────────────────────────────────────────────
def model_registration(
    spec: ModelSpec,
    catalog: str,
    schema: str,
    input_example: Optional[pd.DataFrame] = None,
) -> RegistrationResult:
    """Register one ModelSpec with Champion/Challenger handling."""
    spec.validate()

    client = MlflowClient()
    registered_model_name = _build_model_name(catalog, schema, spec.model_name)

    logger.info("Target model : %s", registered_model_name)
    logger.info("Metric       : %s (%s)",
                spec.metric or spec.metric_pattern, spec.metric_direction)

    new_run, new_run_id = _resolve_source_run(client, spec, input_example)
    new_metric_key, new_metric_value = _resolve_metric(new_run, spec)

    logger.info(
        "New model — run_id: %s | run_name: %s | model_type: %s | %s: %.4f",
        new_run_id,
        new_run.data.tags.get("mlflow.runName", "N/A"),
        new_run.data.tags.get("model_type", "N/A"),
        new_metric_key,
        new_metric_value,
    )

    try:
        existing_versions = _safe_search_model_versions(
            client, f"name='{registered_model_name}'"
        )
        model_exists = len(existing_versions) > 0
    except MlflowException as exc:
        if "RESOURCE_DOES_NOT_EXIST" in str(exc):
            logger.info("Model not yet registered in Unity Catalog.")
            model_exists, existing_versions = False, []
        else:
            raise

    if model_exists and _run_already_registered(
        existing_versions, new_run_id, registered_model_name
    ):
        return RegistrationResult(
            registered_model_name=registered_model_name,
            new_version="N/A",
            new_alias="already_registered",
            champion_version="N/A",
            run_id=new_run_id,
            metric=new_metric_key,
            metric_value=new_metric_value,
            scenario="skipped_duplicate",
        )

    champion_version = (
        _get_champion_version(client, registered_model_name, existing_versions)
        if model_exists else None
    )

    if champion_version is None:
        if model_exists:
            logger.warning("Versions exist but no Champion alias — registering as Champion.")
        else:
            logger.info("SCENARIO: first registration — registering as Champion.")
        result = _register_as_champion(
            client=client,
            run_id=new_run_id,
            registered_model_name=registered_model_name,
            new_run=new_run,
            new_metric_value=new_metric_value,
            metric=new_metric_key,
        )
    else:
        champion_run = _safe_get_run(client, champion_version.run_id)
        # Resolve separately: with window-stamped keys the Champion's metric
        # name differs from the challenger's, even for the same measure.
        champion_metric_key, champion_metric_value = _resolve_metric(champion_run, spec)

        logger.info("Current Champion — version: %s | %s: %.4f",
                    champion_version.version, champion_metric_key, champion_metric_value)
        if champion_metric_key != new_metric_key:
            logger.info(
                "Comparing across differently-stamped keys: new '%s' vs champion '%s'.",
                new_metric_key, champion_metric_key,
            )

        if _is_model_better(
            new_value=new_metric_value,
            champion_value=champion_metric_value,
            metric_direction=spec.metric_direction,
            improvement_threshold=spec.improvement_threshold,
        ):
            logger.info("SCENARIO: new model wins — promoting; old Champion -> Challenger.")
            result = _promote_to_champion(
                client=client,
                run_id=new_run_id,
                registered_model_name=registered_model_name,
                new_run=new_run,
                new_metric_value=new_metric_value,
                metric=new_metric_key,
                old_champion_version=champion_version.version,
            )
        else:
            logger.info("SCENARIO: Champion holds — registering new model as Challenger.")
            result = _register_as_challenger(
                client=client,
                run_id=new_run_id,
                registered_model_name=registered_model_name,
                new_run=new_run,
                new_metric_value=new_metric_value,
                metric=new_metric_key,
                champion_version=champion_version.version,
            )

    _print_final_state(client, registered_model_name, new_metric_key)
    result["registered_model_name"] = registered_model_name
    result["metric"] = new_metric_key
    return result


def run_all_registrations(
    models: Dict[str, ModelSpec],
    catalog: str,
    schema: str,
    input_example: Optional[pd.DataFrame] = None,
    fail_fast: bool = False,
) -> Dict[str, Any]:
    """
    Register every spec. Returns key -> RegistrationResult | {"error": ...}.

    Errors are captured so one bad model does not abort the rest. Pass
    fail_fast=True (or --fail_fast) to let the exception propagate instead —
    useful in a notebook, where a caught error only shows up as SystemExit: 1.
    """
    results: Dict[str, Any] = {}
    for model_key, spec in models.items():
        logger.info("=" * 70)
        logger.info("Processing model key: %s", model_key)
        try:
            results[model_key] = model_registration(
                spec, catalog=catalog, schema=schema, input_example=input_example
            )
        except Exception as exc:
            if fail_fast:
                raise
            logger.error("Registration failed for '%s': %s", model_key, exc, exc_info=True)
            results[model_key] = {"error": str(exc)}
    return results


# ──────────────────────────────────────────────────────────────────────────
# Test window + baseline
# ──────────────────────────────────────────────────────────────────────────
def get_test_ranges(spark: SparkSession, cfg: TableConfig) -> tuple[str, str]:
    """test_end = MAX(event_date); test_start = test_end - cfg.test_months."""
    features = spark.read.table(cfg.feature_table)
    require_columns(features, date_source_columns(cfg), cfg.feature_table)

    raw = (
        with_event_date(features, cfg)
        .select(F.max(F.col(EVENT_DATE_COL)).alias("max_date"))
        .collect()[0]["max_date"]
    )
    if raw is None:
        raise ValueError(
            f"No usable dates in '{cfg.feature_table}' with date_kind="
            f"'{cfg.date_kind}' on {date_source_columns(cfg)}. "
            f"Table may be empty or every date is a sentinel."
        )

    start = (raw - relativedelta(months=cfg.test_months)).strftime("%Y-%m-%d")
    end = raw.strftime("%Y-%m-%d")
    logger.info("Test window — %s -> %s (%d months)", start, end, cfg.test_months)
    return start, end


def build_predict_input(features_pd: pd.DataFrame, cfg: TableConfig) -> pd.DataFrame:
    """Drop identifiers, helper columns and labels before model.predict()."""
    drop = set(cfg.exclude_from_features) | {EVENT_DATE_COL, cfg.label_col}
    return features_pd.drop(columns=[c for c in drop if c in features_pd.columns])


def create_baseline_after_registration(
    spark: SparkSession,
    registration_result: dict,
    cfg: TableConfig,
    test_start_date: str,
    test_end_date: str,
) -> None:
    """Score the test window with the Champion, join actuals, overwrite the baseline."""
    if registration_result.get("scenario") == "skipped_duplicate":
        logger.info("Skipping baseline creation — run already registered.")
        return

    model_name = registration_result["registered_model_name"]
    client = MlflowClient()
    mv = client.get_model_version_by_alias(model_name, "champion")
    model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

    logger.info("Baseline — model: %s | version: %s | window: %s -> %s",
                model_name, mv.version, test_start_date, test_end_date)

    features_df = with_event_date(spark.read.table(cfg.feature_table), cfg).filter(
        (F.col(EVENT_DATE_COL) >= F.to_date(F.lit(test_start_date)))
        & (F.col(EVENT_DATE_COL) <= F.to_date(F.lit(test_end_date)))
    )

    row_count = features_df.count()
    if row_count == 0:
        raise ValueError(
            f"No rows in '{cfg.feature_table}' between {test_start_date} and "
            f"{test_end_date}. Check date_kind and test_months."
        )
    logger.info("Feature rows in test window: %d", row_count)

    features_pd = features_df.toPandas()
    raw_preds = model.predict(build_predict_input(features_pd, cfg))

    if isinstance(raw_preds, pd.DataFrame):
        pred_values = (
            raw_preds[cfg.prediction_col].values
            if cfg.prediction_col in raw_preds.columns
            else raw_preds.iloc[:, 0].values
        )
    else:
        pred_values = np.asarray(raw_preds).ravel()

    if len(pred_values) != len(features_pd):
        raise ValueError(
            f"Model returned {len(pred_values)} predictions for {len(features_pd)} rows."
        )

    features_pd["event_date"] = pd.to_datetime(features_pd[EVENT_DATE_COL])
    features_pd = features_pd.drop(columns=[EVENT_DATE_COL])
    features_pd["prediction_final"] = pred_values.astype(np.float32)
    features_pd["model_version"] = str(mv.version)
    features_pd["model_name"] = model_name
    features_pd["prediction_timestamp"] = datetime.utcnow()

    if cfg.actual_table and spark.catalog.tableExists(cfg.actual_table):
        join_keys = cfg.resolved_join_keys()
        actuals_sdf = spark.read.table(cfg.actual_table)
        require_columns(actuals_sdf, join_keys + [cfg.label_col], cfg.actual_table)

        total = actuals_sdf.count()
        distinct = actuals_sdf.select(*join_keys).distinct().count()
        if total != distinct:
            logger.warning(
                "'%s' has %d rows but only %d distinct %s — the join will fan out "
                "and inflate the baseline. Add columns to join_keys.",
                cfg.actual_table, total, distinct, join_keys,
            )

        actuals_pd = actuals_sdf.select(*join_keys, cfg.label_col).toPandas()
        before = len(features_pd)
        features_pd = features_pd.merge(actuals_pd, on=join_keys, how="left")
        if len(features_pd) != before:
            logger.warning("Join changed row count: %d -> %d.", before, len(features_pd))

        features_pd[cfg.label_col] = features_pd[cfg.label_col].astype(np.float32)
        matched = int(features_pd[cfg.label_col].notna().sum())
        logger.info("Actuals joined: %d / %d rows have '%s'",
                    matched, len(features_pd), cfg.label_col)
        if matched == 0:
            logger.warning(
                "Join on %s matched nothing — check key dtypes on both sides "
                "(a string/bigint mismatch joins to zero rows silently).", join_keys,
            )
    else:
        logger.warning("Actuals table '%s' not found — labels will be NULL.", cfg.actual_table)
        features_pd[cfg.label_col] = np.full(len(features_pd), np.nan, dtype=np.float32)

    (
        spark.createDataFrame(features_pd)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.baseline_table)
    )
    logger.info("Baseline written: '%s' | %d rows | model version %s",
                cfg.baseline_table, len(features_pd), mv.version)


# ──────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", default=DEFAULT_PIPELINE,
                        choices=sorted(PIPELINES), help="Which PIPELINES entry to run.")
    parser.add_argument("--catalog", default=None, help="Override pipeline catalog.")
    parser.add_argument("--schema", default=None, help="Override pipeline schema.")
    parser.add_argument("--experiment_path", default=None,
                        help="Override experiment_path on experiment-sourced models.")
    parser.add_argument("--test_months", type=int, default=None)
    parser.add_argument("--skip_baseline", action="store_true")
    parser.add_argument("--validate_only", action="store_true",
                        help="Check the column contract and exit.")
    parser.add_argument("--fail_fast", action="store_true",
                        help="Raise the first exception instead of capturing it. "
                             "Use in notebooks, where a captured error shows "
                             "only as SystemExit: 1.")
    args, _ = parser.parse_known_args()
    return args


def run_pipeline(spark: SparkSession, pipeline: PipelineConfig,
                 skip_baseline: bool = False, fail_fast: bool = False) -> int:
    """Validate, register every model, refresh baselines. Returns a POSIX exit code."""
    cfg = pipeline.tables
    validate_table_config(spark, cfg)
    test_start_date, test_end_date = get_test_ranges(spark, cfg)

    input_example = None
    if any(s.source == "file" for s in pipeline.models.values()):
        sample = with_event_date(spark.read.table(cfg.feature_table), cfg).limit(5).toPandas()
        input_example = build_predict_input(sample, cfg)

    results = run_all_registrations(
        pipeline.models,
        catalog=pipeline.catalog,
        schema=pipeline.schema,
        input_example=input_example,
        fail_fast=fail_fast,
    )

    failures: List[str] = []
    for key, res in results.items():
        logger.info("Result for %s: %s", key, res)
        if not isinstance(res, dict) or "error" in res:
            failures.append(f"{key}: registration — {res.get('error', 'unknown')}")
            continue
        if skip_baseline or not pipeline.models[key].create_baseline:
            continue
        try:
            create_baseline_after_registration(
                spark=spark,
                registration_result=res,
                cfg=cfg,
                test_start_date=test_start_date,
                test_end_date=test_end_date,
            )
        except Exception as exc:
            if fail_fast:
                raise
            failures.append(f"{key}: baseline — {exc}")
            logger.error("Baseline creation failed for '%s': %s", key, exc, exc_info=True)

    # Spell out why the exit code is non-zero — in a notebook the caller only
    # sees "SystemExit: 1" otherwise.
    logger.info("=" * 70)
    if failures:
        logger.error("Pipeline '%s' finished with %d failure(s):", pipeline.name, len(failures))
        for line in failures:
            logger.error("  - %s", line)
        logger.error("Re-run with fail_fast=True to get the full traceback.")
        return 1

    logger.info("Pipeline '%s' completed successfully.", pipeline.name)
    return 0


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    mlflow.set_registry_uri("databricks-uc")

    pipeline = get_pipeline(args.pipeline)
    logger.info("Pipeline: %s", pipeline.name)

    if args.catalog:
        pipeline.catalog = args.catalog
    if args.schema:
        pipeline.schema = args.schema
    if args.test_months is not None:
        pipeline.tables.test_months = args.test_months
    if args.experiment_path:
        pipeline.experiment_path = args.experiment_path
        for spec in pipeline.models.values():
            if spec.source == "experiment":
                spec.experiment_path = args.experiment_path

    if args.validate_only:
        validate_table_config(spark, pipeline.tables)
        start, end = get_test_ranges(spark, pipeline.tables)
        logger.info("Validation passed. Test window: %s -> %s", start, end)
        return 0

    return run_pipeline(
        spark,
        pipeline,
        skip_baseline=args.skip_baseline,
        fail_fast=args.fail_fast,
    )


if __name__ == "__main__":
    main()
