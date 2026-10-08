"""
promotion_functions.py
----------------------
Helper functions for MLflow Champion/Challenger model management.

"""
import sys
import logging
import time
import functools
from datetime import datetime, timezone
from typing import Optional

import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
sys.path.insert(0, "/Workspace/application/demand_forecasting_spare_parts")
from src.utils.retry import _retry

logger = logging.getLogger(__name__)



@_retry()
def _safe_register_model(model_uri: str, name: str, tags: dict):
    return mlflow.register_model(model_uri=model_uri, name=name, tags=tags)


@_retry()
def _safe_set_alias(client: MlflowClient, name: str, alias: str, version: str):
    client.set_registered_model_alias(name=name, alias=alias, version=version)


@_retry()
def _safe_delete_alias(client: MlflowClient, name: str, alias: str):
    client.delete_registered_model_alias(name=name, alias=alias)


@_retry()
def _safe_update_version(client: MlflowClient, name: str, version: str, description: str):
    client.update_model_version(name=name, version=version, description=description)


@_retry()
def _safe_get_alias(client: MlflowClient, name: str, alias: str):
    return client.get_model_version_by_alias(name=name, alias=alias)


@_retry()
def _safe_search_versions(client: MlflowClient, filter_string: str):
    return client.search_model_versions(filter_string)


@_retry()
def _safe_get_run(client: MlflowClient, run_id: str):
    return client.get_run(run_id)

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _register_as_champion(
    client: MlflowClient,
    run_id: str,
    registered_model_name: str,
    new_run,
    new_metric_value: float,
    metric: str,
) -> dict:
    """Register a model version and assign the 'champion' alias."""

    model_uri = f"runs:/{run_id}/model"
    all_metrics = {k: str(v) for k, v in new_run.data.metrics.items()}
    now = _now_str()

    model_version = _safe_register_model(
        model_uri=model_uri,
        name=registered_model_name,
        tags={
            **all_metrics,
            "model_type": new_run.data.tags.get("model_type", "N/A"),
            "run_name": new_run.data.tags.get("mlflow.runName", "N/A"),
            "role": "champion",
            "registered_date": now,
        },
    )

    _safe_set_alias(client, registered_model_name, "champion", model_version.version)

    _safe_update_version(
        client,
        name=registered_model_name,
        version=model_version.version,
        description=(
            f"Production Champion Model\n\n"
            f"Performance : {metric} = {new_metric_value:.4f}\n"
            f"Registered  : {now}\n"
            f"Run ID      : {run_id}"
        ),
    )

    logger.info(
        "Registered as Champion — version: %s | %s: %.4f",
        model_version.version, metric, new_metric_value,
    )

    return {
        "action": "registered_as_champion",
        "new_version": model_version.version,
        "new_alias": "champion",
        "champion_version": model_version.version,
        "metric_value": new_metric_value,
        "run_id": run_id,
        "scenario": "first_registration",
    }


def _promote_to_champion(
    client: MlflowClient,
    run_id: str,
    registered_model_name: str,
    new_run,
    new_metric_value: float,
    metric: str,
    old_champion_version: str,
) -> dict:
    """Register new version as Champion; demote old Champion to Challenger."""

    model_uri = f"runs:/{run_id}/model"
    all_metrics = {k: str(v) for k, v in new_run.data.metrics.items()}
    now = _now_str()

    model_version = _safe_register_model(
        model_uri=model_uri,
        name=registered_model_name,
        tags={
            **all_metrics,
            "model_type": new_run.data.tags.get("model_type", "N/A"),
            "run_name": new_run.data.tags.get("mlflow.runName", "N/A"),
            "role": "champion",
            "promoted_from": "new_registration",
            "replaced_version": str(old_champion_version),
            "registered_date": now,
        },
    )

    # Order matters: delete old alias before setting new one
    _safe_delete_alias(client, registered_model_name, "champion")
    _safe_set_alias(client, registered_model_name, "champion", model_version.version)

    # Demote previous champion to challenger
    # (Remove stale challenger alias first if it exists)
    try:
        _safe_delete_alias(client, registered_model_name, "challenger")
    except MlflowException as exc:
        logger.debug("No existing 'challenger' alias to remove: %s", exc)

    _safe_set_alias(client, registered_model_name, "challenger", old_champion_version)

    _safe_update_version(
        client,
        name=registered_model_name,
        version=model_version.version,
        description=(
            f"Production Champion Model (Promoted)\n\n"
            f"Performance     : {metric} = {new_metric_value:.4f}\n"
            f"Promoted        : {now}\n"
            f"Replaced version: {old_champion_version}\n"
            f"Run ID          : {run_id}"
        ),
    )

    logger.info(
        "Promoted to Champion — new version: %s | demoted version: %s | %s: %.4f",
        model_version.version, old_champion_version, metric, new_metric_value,
    )

    return {
        "action": "promoted_to_champion",
        "new_version": model_version.version,
        "new_alias": "champion",
        "champion_version": model_version.version,
        "demoted_version": old_champion_version,
        "metric_value": new_metric_value,
        "run_id": run_id,
        "scenario": "promoted",
    }


def _register_as_challenger(
    client: MlflowClient,
    run_id: str,
    registered_model_name: str,
    new_run,
    new_metric_value: float,
    metric: str,
    champion_version: str,
) -> dict:
    """Register new version as Challenger; Champion is untouched."""

    model_uri = f"runs:/{run_id}/model"
    all_metrics = {k: str(v) for k, v in new_run.data.metrics.items()}
    now = _now_str()

    model_version = _safe_register_model(
        model_uri=model_uri,
        name=registered_model_name,
        tags={
            **all_metrics,
            "model_type": new_run.data.tags.get("model_type", "N/A"),
            "run_name": new_run.data.tags.get("mlflow.runName", "N/A"),
            "role": "challenger",
            "current_champion_version": str(champion_version),
            "registered_date": now,
        },
    )

    # Remove stale challenger alias if it exists
    try:
        _safe_delete_alias(client, registered_model_name, "challenger")
    except MlflowException as exc:
        logger.debug("No existing 'challenger' alias to remove: %s", exc)

    _safe_set_alias(client, registered_model_name, "challenger", model_version.version)

    _safe_update_version(
        client,
        name=registered_model_name,
        version=model_version.version,
        description=(
            f"Challenger Model (Did not outperform Champion)\n\n"
            f"Performance      : {metric} = {new_metric_value:.4f}\n"
            f"Registered       : {now}\n"
            f"Current Champion : Version {champion_version}\n"
            f"Run ID           : {run_id}"
        ),
    )

    logger.info(
        "Registered as Challenger — version: %s | champion remains: %s | %s: %.4f",
        model_version.version, champion_version, metric, new_metric_value,
    )

    return {
        "action": "registered_as_challenger",
        "new_version": model_version.version,
        "new_alias": "challenger",
        "champion_version": champion_version,
        "metric_value": new_metric_value,
        "run_id": run_id,
        "scenario": "challenger",
    }


def _get_champion_version(client: MlflowClient, registered_model_name: str, existing_versions):
    """
    Return the ModelVersion object that holds the 'champion' alias, or None.

    Uses the alias API directly; existing_versions is accepted for
    call-site compatibility but not used (alias lookup is authoritative).
    """
    try:
        champion_version = _safe_get_alias(client, registered_model_name, "champion")
        logger.info("Found Champion: version %s", champion_version.version)
        return champion_version
    except MlflowException as exc:
        logger.info("No 'champion' alias found for '%s': %s", registered_model_name, exc)
        return None


def _is_model_better(
    new_value: float,
    champion_value: float,
    metric_direction: str,
    improvement_threshold: float,
) -> bool:
    """
    Return True if new_value beats champion_value by at least improvement_threshold.

    improvement_threshold is a fractional value (0.02 = 2%).
    Guards against ZeroDivisionError when champion_value is 0.
    """
    if champion_value is None:
        return True
    else:
        if champion_value == 0:
            logger.warning(
                "Champion metric value is 0 — cannot compute relative improvement. "
                "Falling back to absolute comparison."
            )
            if metric_direction == "minimize":
                return new_value < champion_value
            else:
                return new_value > champion_value

        if metric_direction == "minimize":
            improvement_pct = (champion_value - new_value) / abs(champion_value)
        else:
            improvement_pct = (new_value - champion_value) / abs(champion_value)

    return improvement_pct >= improvement_threshold


def _print_final_state(
    client: MlflowClient,
    registered_model_name: str,
    metric: str,
) -> None:
    """
    Log a summary table of all registered versions with their aliases and metric values.

    Signature change from prototype: takes (client, registered_model_name, metric)
    instead of (result). Caller in model_registration.py must be updated to match.
    """
    logger.info("=" * 80)
    logger.info("FINAL MODEL REGISTRY STATE — %s", registered_model_name)
    logger.info("=" * 80)

    all_versions = _safe_search_versions(client, f"name='{registered_model_name}'")

    if not all_versions:
        logger.info("No versions found.")
        return

    summary_data = []
    for version in all_versions:
        try:
            run = _safe_get_run(client, version.run_id)
            metric_value = run.data.metrics.get(metric)
            metric_str = f"{metric_value:.4f}" if metric_value is not None else "N/A"
        except Exception as exc:
            logger.warning("Could not fetch run for version %s: %s", version.version, exc)
            metric_str = "N/A"

        summary_data.append({
            "Version": version.version,
            metric: metric_str,
            "Created": pd.to_datetime(version.creation_timestamp, unit="ms").strftime(
                "%Y-%m-%d %H:%M"
            ),
            "Run ID": version.run_id[:8] + "...",
        })

    summary_df = pd.DataFrame(summary_data).sort_values("Version", ascending=False)
    logger.info("\n%s", summary_df.to_string(index=False))