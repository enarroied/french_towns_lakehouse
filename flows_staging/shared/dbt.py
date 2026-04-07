import subprocess

from flows_staging.shared.config import DBT_PROFILES_ARGS
from flows_staging.shared.config import DBT_PROJECT_DIR


def run_dbt_command(
    args: list[str], failure_message: str
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["dbt"] + args + DBT_PROFILES_ARGS,
        cwd=DBT_PROJECT_DIR,
        check=False,
        capture_output=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(failure_message)
    return result


def stage_external_sources() -> None:
    run_dbt_command(
        ["run-operation", "stage_external_sources"],
        "dbt stage_external_sources failed — check logs above",
    )


def run_models(model_selector: str) -> None:
    run_dbt_command(
        ["run", "--select", model_selector],
        f"dbt run --select {model_selector} failed — check logs above",
    )


def test_models(model_selector: str) -> None:
    run_dbt_command(
        ["test", "--select", model_selector],
        f"dbt test --select {model_selector} failed — check logs above",
    )


def run_and_test(model_selector: str) -> None:
    run_models(model_selector)
    test_models(model_selector)
