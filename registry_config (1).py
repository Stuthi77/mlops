# """
# registry_config.py
# ------------------
# Declarative config for the registration pipeline. Nothing else in the codebase
# hardcodes a table, column or experiment path.

# Structure
#     PipelineConfig   one use case: catalog, schema, experiment, tables, models
#     TableConfig      table names + the column contract
#     ModelSpec        one registrable model (experiment / run / serialized file)
#     PIPELINES        name -> PipelineConfig

# Adding another dataset of the same shape = copy one PIPELINES entry, change
# the table names, run with --pipeline <name>. No code changes.

# Date handling note for this workspace
#     sample_data_prepared has no date column. It stores REQ_YEAR / REQ_MONTH /
#     REQ_DAY as separate INTs, so date_kind="parts" composes them with
#     try_make_date(). Rows carrying the 1900-01-01 sentinel are excluded from
#     the test window via min_valid_date.
# """
# from __future__ import annotations

# from dataclasses import dataclass, field
# from typing import Any, Dict, List, Optional, Sequence


# # ──────────────────────────────────────────────────────────────────────────
# # Table + column contract
# # ──────────────────────────────────────────────────────────────────────────
# @dataclass
# class TableConfig:
#     """Where data lives and what the columns are called."""

#     feature_table: str
#     baseline_table: str
#     actual_table: Optional[str] = None

#     # ---- how to obtain an event DATE from the feature table ----
#     #   "auto"          date_col is already DATE/TIMESTAMP, or STRING+date_format
#     #   "parts"         compose from date_parts = (year_col, month_col, day_col)
#     #   "yyyymmdd_int"  INT packed as 20240131
#     #   "unix_days"     INT days since 1970-01-01
#     #   "unix_seconds"  INT/BIGINT epoch seconds
#     date_kind: str = "auto"

#     # Used by every kind except "parts".
#     date_col: Optional[str] = None

#     # Used only when date_kind="parts": (year_col, month_col, day_col).
#     date_parts: Optional[Sequence[str]] = None

#     # Used only when date_kind="auto" and the column is a STRING.
#     date_format: Optional[str] = None

#     # Rows with a composed date before this are treated as missing and
#     # excluded from the test window. Set to None to keep everything.
#     min_valid_date: Optional[str] = None

#     # ---- joining predictions to actuals ----
#     join_keys: List[str] = field(default_factory=list)
#     label_col: str = "target"

#     # ---- scoring ----
#     prediction_col: str = "prediction_raw"
#     test_months: int = 3

#     # Columns dropped before model.predict() — IDs, labels, leakage.
#     exclude_from_features: List[str] = field(default_factory=list)

#     def validate(self) -> None:
#         valid_kinds = ("auto", "parts", "yyyymmdd_int", "unix_days", "unix_seconds")
#         if self.date_kind not in valid_kinds:
#             raise ValueError(f"date_kind must be one of {valid_kinds}, got '{self.date_kind}'")

#         if self.date_kind == "parts":
#             if not self.date_parts or len(self.date_parts) != 3:
#                 raise ValueError(
#                     "date_kind='parts' requires date_parts=(year_col, month_col, day_col)"
#                 )
#         elif not self.date_col:
#             raise ValueError(f"date_kind='{self.date_kind}' requires date_col")

#         if self.feature_table == self.baseline_table:
#             raise ValueError(
#                 f"baseline_table must differ from feature_table — the baseline is "
#                 f"overwritten on every run and would destroy '{self.feature_table}'."
#             )
#         if self.actual_table and self.actual_table == self.baseline_table:
#             raise ValueError("baseline_table must differ from actual_table.")

#     def resolved_join_keys(self) -> List[str]:
#         if not self.join_keys:
#             raise ValueError("TableConfig.join_keys is empty — set the shared grain.")
#         return self.join_keys


# # ──────────────────────────────────────────────────────────────────────────
# # One registrable model
# # ──────────────────────────────────────────────────────────────────────────
# @dataclass
# class ModelSpec:
#     """
#     source="experiment" -> best run in experiment_path by `metric`
#     source="run"        -> a specific run_id
#     source="file"       -> serialized file (.pkl .joblib .ubj .json .bst
#                            .cb .cbm .txt); logged to a fresh run, then registered
#     """

#     model_name: str                       # UC name without catalog.schema
#     metric: str = "test_rmse"
#     metric_direction: str = "minimize"    # "minimize" | "maximize"
#     improvement_threshold: float = 0.0    # 0.02 == must be 2% better to win

#     source: str = "experiment"
#     experiment_path: Optional[str] = None  # filled from PipelineConfig if blank
#     run_id: Optional[str] = None

#     # --- source="file" only ---
#     model_path: Optional[str] = None
#     model_format: Optional[str] = None     # override extension sniffing
#     loader_kwargs: Dict[str, Any] = field(default_factory=dict)
#     metrics: Dict[str, float] = field(default_factory=dict)
#     params: Dict[str, Any] = field(default_factory=dict)
#     tags: Dict[str, str] = field(default_factory=dict)

#     create_baseline: bool = True

#     def validate(self) -> None:
#         if self.metric_direction not in ("minimize", "maximize"):
#             raise ValueError(
#                 f"[{self.model_name}] metric_direction must be 'minimize' or 'maximize'"
#             )
#         if self.improvement_threshold < 0:
#             raise ValueError(f"[{self.model_name}] improvement_threshold must be >= 0")
#         if self.source not in ("experiment", "run", "file"):
#             raise ValueError(f"[{self.model_name}] source must be experiment|run|file")
#         if self.source == "experiment" and not self.experiment_path:
#             raise ValueError(f"[{self.model_name}] source='experiment' needs experiment_path")
#         if self.source == "run" and not self.run_id:
#             raise ValueError(f"[{self.model_name}] source='run' needs run_id")
#         if self.source == "file":
#             if not self.model_path:
#                 raise ValueError(f"[{self.model_name}] source='file' needs model_path")
#             if self.metric not in self.metrics:
#                 raise ValueError(
#                     f"[{self.model_name}] source='file' needs metrics['{self.metric}'] "
#                     f"to compare against the Champion. Got: {sorted(self.metrics)}"
#                 )


# # ──────────────────────────────────────────────────────────────────────────
# # One use case
# # ──────────────────────────────────────────────────────────────────────────
# @dataclass
# class PipelineConfig:
#     name: str
#     catalog: str
#     schema: str
#     experiment_path: str
#     tables: TableConfig
#     models: Dict[str, ModelSpec]

#     def validate(self) -> None:
#         self.tables.validate()
#         for key, spec in self.models.items():
#             if spec.source == "experiment" and not spec.experiment_path:
#                 spec.experiment_path = self.experiment_path   # inherit
#             spec.validate()
#         if not self.models:
#             raise ValueError(f"Pipeline '{self.name}' has no models configured.")


# # ══════════════════════════════════════════════════════════════════════════
# # PIPELINES — add a new use case by copying a block
# # ══════════════════════════════════════════════════════════════════════════
# PIPELINES: Dict[str, PipelineConfig] = {

#     # ── PO total cost ─────────────────────────────────────────────────────
#     "total_cost": PipelineConfig(
#         name="total_cost",
#         catalog="dev_platform",
#         schema="mlops",
#         experiment_path="/Workspace/Users/danduprolu.stuthi@latentview.com/sample_model/",
#         tables=TableConfig(
#             feature_table="dev_platform.mlops.sample_data_prepared",
#             actual_table="dev_platform.mlops.ground_truth_total_cost",
#             # NOT sample_data — that is the raw source table and would be
#             # destroyed by the overwrite.
#             baseline_table="dev_platform.mlops.total_cost_baseline",

#             date_kind="parts",
#             date_parts=("REQ_YEAR", "REQ_MONTH", "REQ_DAY"),
#             min_valid_date="1990-01-01",       # drops the 1900-01-01 sentinel

#             join_keys=["PONUMBER"],
#             label_col="ground_truth_total_cost",

#             prediction_col="prediction_raw",
#             test_months=2,
#             # PONUMBER is an identifier. Remove it here if it was a training feature.
#             exclude_from_features=["PONUMBER"],
#         ),
#         models={
#             "total_cost_best_run": ModelSpec(
#                 model_name="total_cost_model",
#                 metric="test_rmse",
#                 metric_direction="minimize",
#                 improvement_threshold=0.0,
#                 source="experiment",
#             ),
#             # "total_cost_xgb_file": ModelSpec(
#             #     model_name="total_cost_xgb",
#             #     metric="test_rmse",
#             #     source="file",
#             #     model_path="/Volumes/dev_platform/mlops/models/total_cost.ubj",
#             #     metrics={"test_rmse": 12.34},
#             #     tags={"model_type": "xgboost"},
#             # ),
#             # "total_cost_cb_file": ModelSpec(
#             #     model_name="total_cost_cb",
#             #     metric="test_rmse",
#             #     source="file",
#             #     model_path="/Volumes/dev_platform/mlops/models/total_cost.cb",
#             #     loader_kwargs={"estimator": "regressor"},
#             #     metrics={"test_rmse": 11.90},
#             # ),
#         },
#     ),

#     # ── Delay in days — same tables, different target ─────────────────────
#     # Template for the "same use case, different dataset" pattern.
#     "delay_days": PipelineConfig(
#         name="delay_days",
#         catalog="dev_platform",
#         schema="mlops",
#         experiment_path="/Workspace/Users/danduprolu.stuthi@latentview.com/delay_model/",
#         tables=TableConfig(
#             feature_table="dev_platform.mlops.sample_data_prepared",
#             actual_table="dev_platform.mlops.ground_truth_delay_days",
#             baseline_table="dev_platform.mlops.delay_days_baseline",

#             date_kind="parts",
#             date_parts=("REQ_YEAR", "REQ_MONTH", "REQ_DAY"),
#             min_valid_date="1990-01-01",

#             join_keys=["PONUMBER"],
#             label_col="ground_truth_delay_days",

#             test_months=2,
#             exclude_from_features=["PONUMBER", "delay_in_days"],
#         ),
#         models={
#             "delay_best_run": ModelSpec(
#                 model_name="delay_days_model",
#                 metric="test_mae",
#                 metric_direction="minimize",
#                 source="experiment",
#             ),
#         },
#     ),
# }

# DEFAULT_PIPELINE = "total_cost"


# def get_pipeline(name: Optional[str] = None) -> PipelineConfig:
#     """Fetch and validate a pipeline by name."""
#     key = name or DEFAULT_PIPELINE
#     try:
#         pipeline = PIPELINES[key]
#     except KeyError:
#         raise ValueError(
#             f"Unknown pipeline '{key}'. Configured: {sorted(PIPELINES)}"
#         ) from None
#     pipeline.validate()
#     return pipeline




"""
registry_config.py
------------------
Declarative config for the registration pipeline. Nothing else in the codebase
hardcodes a table, column or experiment path.

Structure
    PipelineConfig   one use case: catalog, schema, experiment, tables, models
    TableConfig      table names + the column contract
    ModelSpec        one registrable model (experiment / run / serialized file)
    PIPELINES        name -> PipelineConfig

Adding another dataset of the same shape = copy one PIPELINES entry, change
the table names, run with --pipeline <name>. No code changes.

Date handling note for this workspace
    sample_data_prepared has no date column. It stores REQ_YEAR / REQ_MONTH /
    REQ_DAY as separate INTs, so date_kind="parts" composes them with
    try_make_date(). Rows carrying the 1900-01-01 sentinel are excluded from
    the test window via min_valid_date.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence


# ──────────────────────────────────────────────────────────────────────────
# Table + column contract
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class TableConfig:
    """Where data lives and what the columns are called."""

    feature_table: str
    baseline_table: str
    actual_table: Optional[str] = None

    # ---- how to obtain an event DATE from the feature table ----
    #   "auto"          date_col is already DATE/TIMESTAMP, or STRING+date_format
    #   "parts"         compose from date_parts = (year_col, month_col, day_col)
    #   "yyyymmdd_int"  INT packed as 20240131
    #   "unix_days"     INT days since 1970-01-01
    #   "unix_seconds"  INT/BIGINT epoch seconds
    date_kind: str = "auto"

    # Used by every kind except "parts".
    date_col: Optional[str] = None

    # Used only when date_kind="parts": (year_col, month_col, day_col).
    date_parts: Optional[Sequence[str]] = None

    # Used only when date_kind="auto" and the column is a STRING.
    date_format: Optional[str] = None

    # Rows with a composed date before this are treated as missing and
    # excluded from the test window. Set to None to keep everything.
    min_valid_date: Optional[str] = None

    # ---- joining predictions to actuals ----
    join_keys: List[str] = field(default_factory=list)
    label_col: str = "target"

    # ---- scoring ----
    prediction_col: str = "prediction_raw"
    test_months: int = 3

    # Columns dropped before model.predict() — IDs, labels, leakage.
    exclude_from_features: List[str] = field(default_factory=list)

    def validate(self) -> None:
        valid_kinds = ("auto", "parts", "yyyymmdd_int", "unix_days", "unix_seconds")
        if self.date_kind not in valid_kinds:
            raise ValueError(f"date_kind must be one of {valid_kinds}, got '{self.date_kind}'")

        if self.date_kind == "parts":
            if not self.date_parts or len(self.date_parts) != 3:
                raise ValueError(
                    "date_kind='parts' requires date_parts=(year_col, month_col, day_col)"
                )
        elif not self.date_col:
            raise ValueError(f"date_kind='{self.date_kind}' requires date_col")

        if self.feature_table == self.baseline_table:
            raise ValueError(
                f"baseline_table must differ from feature_table — the baseline is "
                f"overwritten on every run and would destroy '{self.feature_table}'."
            )
        if self.actual_table and self.actual_table == self.baseline_table:
            raise ValueError("baseline_table must differ from actual_table.")

    def resolved_join_keys(self) -> List[str]:
        if not self.join_keys:
            raise ValueError("TableConfig.join_keys is empty — set the shared grain.")
        return self.join_keys


# ──────────────────────────────────────────────────────────────────────────
# One registrable model
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class ModelSpec:
    """
    source="experiment" -> best run in experiment_path by `metric`
    source="run"        -> a specific run_id
    source="file"       -> serialized file (.pkl .joblib .ubj .json .bst
                           .cb .cbm .txt); logged to a fresh run, then registered
    """

    model_name: str                       # UC name without catalog.schema

    # Give metric OR metric_pattern (pattern wins when the exact name is absent).
    #
    # metric_pattern exists because runs often log window-stamped metric keys
    # like "rmse_val_2025-01-10_2025-03-07" — the name changes every run, so no
    # fixed string can match. Anchor the pattern so near-misses don't match:
    # "^rmse_val_" does NOT match "wmape_val_...".
    metric: Optional[str] = None
    metric_pattern: Optional[str] = None

    # Which key to use when the pattern matches several. "latest" takes the
    # last after sorting, which is the most recent window for ISO date stamps.
    metric_select: str = "latest"         # "latest" | "first"

    metric_direction: str = "minimize"    # "minimize" | "maximize"
    improvement_threshold: float = 0.0    # 0.02 == must be 2% better to win

    # Runs pulled from the experiment before picking the best. Needed because
    # a pattern metric cannot be sorted server-side.
    search_max_results: int = 200

    source: str = "experiment"
    experiment_path: Optional[str] = None  # filled from PipelineConfig if blank
    run_id: Optional[str] = None

    # --- source="file" only ---
    model_path: Optional[str] = None
    model_format: Optional[str] = None     # override extension sniffing
    loader_kwargs: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)

    create_baseline: bool = True

    def validate(self) -> None:
        if self.metric_direction not in ("minimize", "maximize"):
            raise ValueError(
                f"[{self.model_name}] metric_direction must be 'minimize' or 'maximize'"
            )
        if self.improvement_threshold < 0:
            raise ValueError(f"[{self.model_name}] improvement_threshold must be >= 0")
        if not self.metric and not self.metric_pattern:
            raise ValueError(
                f"[{self.model_name}] set metric (exact key) or metric_pattern "
                f"(regex, e.g. '^rmse_val_') — one is required."
            )
        if self.metric_select not in ("latest", "first"):
            raise ValueError(f"[{self.model_name}] metric_select must be 'latest' or 'first'")
        if self.source not in ("experiment", "run", "file"):
            raise ValueError(f"[{self.model_name}] source must be experiment|run|file")
        if self.source == "experiment" and not self.experiment_path:
            raise ValueError(f"[{self.model_name}] source='experiment' needs experiment_path")
        if self.source == "run" and not self.run_id:
            raise ValueError(f"[{self.model_name}] source='run' needs run_id")
        if self.source == "file":
            if not self.model_path:
                raise ValueError(f"[{self.model_name}] source='file' needs model_path")
            if not self.metric:
                raise ValueError(
                    f"[{self.model_name}] source='file' needs an exact metric name "
                    f"(metric_pattern cannot be matched against a dict you supply)."
                )
            if self.metric not in self.metrics:
                raise ValueError(
                    f"[{self.model_name}] source='file' needs metrics['{self.metric}'] "
                    f"to compare against the Champion. Got: {sorted(self.metrics)}"
                )


# ──────────────────────────────────────────────────────────────────────────
# One use case
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class PipelineConfig:
    name: str
    catalog: str
    schema: str
    experiment_path: str
    tables: TableConfig
    models: Dict[str, ModelSpec]

    def validate(self) -> None:
        self.tables.validate()
        for key, spec in self.models.items():
            if spec.source == "experiment" and not spec.experiment_path:
                spec.experiment_path = self.experiment_path   # inherit
            spec.validate()
        if not self.models:
            raise ValueError(f"Pipeline '{self.name}' has no models configured.")


# ══════════════════════════════════════════════════════════════════════════
# PIPELINES — add a new use case by copying a block
# ══════════════════════════════════════════════════════════════════════════
PIPELINES: Dict[str, PipelineConfig] = {

    # ── PO total cost ─────────────────────────────────────────────────────
    "total_cost": PipelineConfig(
        name="total_cost",
        catalog="dev_platform",
        schema="mlops",
        experiment_path="/Workspace/Users/danduprolu.stuthi@latentview.com/sample_model/",
        tables=TableConfig(
            feature_table="dev_platform.mlops.sample_data_prepared",
            actual_table="dev_platform.mlops.ground_truth_total_cost",
            # NOT sample_data — that is the raw source table and would be
            # destroyed by the overwrite.
            baseline_table="dev_platform.mlops.total_cost_baseline",

            date_kind="parts",
            date_parts=("REQ_YEAR", "REQ_MONTH", "REQ_DAY"),
            min_valid_date="1990-01-01",       # drops the 1900-01-01 sentinel

            join_keys=["PONUMBER"],
            label_col="ground_truth_total_cost",

            prediction_col="prediction_raw",
            test_months=2,
            # PONUMBER is an identifier. Remove it here if it was a training feature.
            exclude_from_features=["PONUMBER"],
        ),
        models={
            "total_cost_best_run": ModelSpec(
                model_name="total_cost_model",
                # Runs log window-stamped keys, e.g.
                #   rmse_val_2025-01-10_2025-03-07
                # so match by pattern instead of a fixed name. The ^ anchor
                # keeps "^rmse_val_" from matching "wmape_val_...".
                metric_pattern=r"^rmse_val_",
                metric_select="latest",
                metric_direction="minimize",
                improvement_threshold=0.0,
                source="experiment",
            ),
            # "total_cost_xgb_file": ModelSpec(
            #     model_name="total_cost_xgb",
            #     metric="test_rmse",
            #     source="file",
            #     model_path="/Volumes/dev_platform/mlops/models/total_cost.ubj",
            #     metrics={"test_rmse": 12.34},
            #     tags={"model_type": "xgboost"},
            # ),
            # "total_cost_cb_file": ModelSpec(
            #     model_name="total_cost_cb",
            #     metric="test_rmse",
            #     source="file",
            #     model_path="/Volumes/dev_platform/mlops/models/total_cost.cb",
            #     loader_kwargs={"estimator": "regressor"},
            #     metrics={"test_rmse": 11.90},
            # ),
        },
    ),

    # ── Delay in days — same tables, different target ─────────────────────
    # Template for the "same use case, different dataset" pattern.
    "delay_days": PipelineConfig(
        name="delay_days",
        catalog="dev_platform",
        schema="mlops",
        experiment_path="/Workspace/Users/danduprolu.stuthi@latentview.com/delay_model/",
        tables=TableConfig(
            feature_table="dev_platform.mlops.sample_data_prepared",
            actual_table="dev_platform.mlops.ground_truth_delay_days",
            baseline_table="dev_platform.mlops.delay_days_baseline",

            date_kind="parts",
            date_parts=("REQ_YEAR", "REQ_MONTH", "REQ_DAY"),
            min_valid_date="1990-01-01",

            join_keys=["PONUMBER"],
            label_col="ground_truth_delay_days",

            test_months=2,
            exclude_from_features=["PONUMBER", "delay_in_days"],
        ),
        models={
            "delay_best_run": ModelSpec(
                model_name="delay_days_model",
                metric_pattern=r"^mae_val_",
                metric_direction="minimize",
                source="experiment",
            ),
        },
    ),
}

DEFAULT_PIPELINE = "total_cost"


def get_pipeline(name: Optional[str] = None) -> PipelineConfig:
    """Fetch and validate a pipeline by name."""
    key = name or DEFAULT_PIPELINE
    try:
        pipeline = PIPELINES[key]
    except KeyError:
        raise ValueError(
            f"Unknown pipeline '{key}'. Configured: {sorted(PIPELINES)}"
        ) from None
    pipeline.validate()
    return pipeline