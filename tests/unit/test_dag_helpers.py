"""Unit tests for the helpers behind the retail DAG (no Airflow or Docker needed)."""

from datetime import date, datetime, timezone

import pytest

from dags.retail_helpers import build_spark_command, resolve_batch_date

TODAY = date(2026, 11, 2)
LOGICAL = datetime(2025, 1, 15, 3, 0, tzinfo=timezone.utc)


def test_explicit_config_wins_over_everything():
    assert resolve_batch_date({"batch_date": "2024-07-01"}, LOGICAL, TODAY) == "2024-07-01"


def test_logical_date_is_used_when_the_config_has_no_date():
    assert resolve_batch_date({}, LOGICAL, TODAY) == "2025-01-15"


def test_today_is_the_last_resort():
    assert resolve_batch_date(None, None, TODAY) == "2026-11-02"
    assert resolve_batch_date({}, None, TODAY) == "2026-11-02"


def test_a_value_that_is_not_a_date_is_rejected():
    with pytest.raises(ValueError):
        resolve_batch_date({"batch_date": "not-a-date"}, LOGICAL, TODAY)


def test_spark_command_matches_what_submit_sh_runs():
    command = build_spark_command("pipelines/pipeline1_ingest.py", "2024-07-01")
    assert command[0] == "spark-submit"
    assert "--packages" in command
    assert command[-3:] == ["pipelines/pipeline1_ingest.py", "--batch-date", "2024-07-01"]