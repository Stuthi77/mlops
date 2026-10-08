"""
model_formats.py
----------------
Registry mapping serialized-model file extensions to (loader, MLflow flavor).

Supported out of the box
    .pkl .pickle .joblib   -> generic / sklearn-style  -> mlflow.sklearn
    .ubj .json .bst .model -> XGBoost                  -> mlflow.xgboost
    .cb  .cbm              -> CatBoost                 -> mlflow.catboost
    .txt                   -> LightGBM Booster         -> mlflow.lightgbm

Add a new format by appending one ModelFormat to FORMATS. Nothing else changes.

All heavy imports are lazy so a cluster without, say, catboost installed can
still import this module.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Loaders
# ──────────────────────────────────────────────────────────────────────────
def _load_pickle(path: str, **kwargs) -> Any:
    """cloudpickle first (handles custom classes / pyfunc wrappers), then pickle."""
    try:
        import cloudpickle as pickle_mod
    except ImportError:  # pragma: no cover
        import pickle as pickle_mod  # type: ignore
    with open(path, "rb") as fh:
        return pickle_mod.load(fh)


def _load_joblib(path: str, **kwargs) -> Any:
    import joblib
    return joblib.load(path)


def _load_xgboost(path: str, sklearn_api: bool = False, **kwargs) -> Any:
    """
    sklearn_api=False -> xgboost.Booster      (works for .ubj/.json/.bst)
    sklearn_api=True  -> XGBRegressor/Classifier, needed if you rely on
                         .predict_proba or sklearn-style params downstream.
    """
    import xgboost as xgb

    if sklearn_api:
        estimator = kwargs.get("estimator", "regressor")
        model = xgb.XGBClassifier() if estimator == "classifier" else xgb.XGBRegressor()
        model.load_model(path)
        return model

    booster = xgb.Booster()
    booster.load_model(path)
    return booster


def _load_catboost(path: str, estimator: str = "regressor", **kwargs) -> Any:
    from catboost import CatBoostClassifier, CatBoostRegressor

    model = CatBoostClassifier() if estimator == "classifier" else CatBoostRegressor()
    model.load_model(path)
    return model


def _load_lightgbm(path: str, **kwargs) -> Any:
    import lightgbm as lgb
    return lgb.Booster(model_file=path)


# ──────────────────────────────────────────────────────────────────────────
# Flavor accessors (lazy — importing mlflow.catboost pulls in catboost)
# ──────────────────────────────────────────────────────────────────────────
def _flavor_sklearn():
    import mlflow.sklearn
    return mlflow.sklearn


def _flavor_xgboost():
    import mlflow.xgboost
    return mlflow.xgboost


def _flavor_catboost():
    import mlflow.catboost
    return mlflow.catboost


def _flavor_lightgbm():
    import mlflow.lightgbm
    return mlflow.lightgbm


# ──────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ModelFormat:
    name: str
    extensions: Tuple[str, ...]
    load: Callable[..., Any]
    flavor: Callable[[], Any]
    pip_requirements: Tuple[str, ...] = ()


FORMATS: Tuple[ModelFormat, ...] = (
    ModelFormat(
        name="pickle",
        extensions=(".pkl", ".pickle"),
        load=_load_pickle,
        flavor=_flavor_sklearn,
        pip_requirements=("scikit-learn", "cloudpickle"),
    ),
    ModelFormat(
        name="joblib",
        extensions=(".joblib",),
        load=_load_joblib,
        flavor=_flavor_sklearn,
        pip_requirements=("scikit-learn", "joblib"),
    ),
    ModelFormat(
        name="xgboost",
        extensions=(".ubj", ".json", ".bst", ".model"),
        load=_load_xgboost,
        flavor=_flavor_xgboost,
        pip_requirements=("xgboost",),
    ),
    ModelFormat(
        name="catboost",
        extensions=(".cb", ".cbm"),
        load=_load_catboost,
        flavor=_flavor_catboost,
        pip_requirements=("catboost",),
    ),
    ModelFormat(
        name="lightgbm",
        extensions=(".txt",),
        load=_load_lightgbm,
        flavor=_flavor_lightgbm,
        pip_requirements=("lightgbm",),
    ),
)

_BY_EXT: Dict[str, ModelFormat] = {
    ext: fmt for fmt in FORMATS for ext in fmt.extensions
}
_BY_NAME: Dict[str, ModelFormat] = {fmt.name: fmt for fmt in FORMATS}

SUPPORTED_EXTENSIONS = tuple(sorted(_BY_EXT))


def resolve_format(path: str, format_name: Optional[str] = None) -> ModelFormat:
    """
    Pick a ModelFormat for `path`.

    `format_name` overrides extension sniffing — necessary for ambiguous
    extensions (.json is XGBoost here, .txt is LightGBM) or extensionless files.
    """
    if format_name:
        try:
            return _BY_NAME[format_name]
        except KeyError:
            raise ValueError(
                f"Unknown model format '{format_name}'. "
                f"Known formats: {sorted(_BY_NAME)}"
            ) from None

    ext = Path(path).suffix.lower()
    try:
        return _BY_EXT[ext]
    except KeyError:
        raise ValueError(
            f"Cannot infer model format from '{path}' (extension '{ext}'). "
            f"Supported extensions: {list(SUPPORTED_EXTENSIONS)}. "
            f"Pass format_name=... to override."
        ) from None


def load_model_file(
    path: str,
    format_name: Optional[str] = None,
    loader_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, ModelFormat]:
    """Load a serialized model from disk. Returns (model_object, format)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Model file not found: {path}")

    fmt = resolve_format(path, format_name)
    logger.info("Loading %s model from %s (format: %s)", fmt.name, path, fmt.name)
    model = fmt.load(str(p), **(loader_kwargs or {}))
    return model, fmt


def log_model_object(
    model: Any,
    fmt: ModelFormat,
    artifact_path: str = "model",
    input_example: Any = None,
    signature: Any = None,
    extra_pip_requirements: Optional[Sequence[str]] = None,
):
    """
    Log `model` under the flavor `fmt` declares, tolerating both the old
    (artifact_path=) and new (name=) MLflow log_model signatures.
    """
    flavor = fmt.flavor()
    kwargs: Dict[str, Any] = {}
    if input_example is not None:
        kwargs["input_example"] = input_example
    if signature is not None:
        kwargs["signature"] = signature
    if extra_pip_requirements:
        kwargs["extra_pip_requirements"] = list(extra_pip_requirements)

    try:
        return flavor.log_model(model, name=artifact_path, **kwargs)
    except TypeError:
        return flavor.log_model(model, artifact_path=artifact_path, **kwargs)