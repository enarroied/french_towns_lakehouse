import os
import subprocess

from flows_staging.shared.config import DBT_PROFILES_ARGS
from flows_staging.shared.config import DBT_PROJECT_DIR


def run_dbt_command(
    args: list[str], failure_message: str
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    try:
        return subprocess.run(
            ["dbt"] + args + DBT_PROFILES_ARGS,
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            env=env,
            check=True,  # Raises CalledProcessError if returncode != 0
        )
    except subprocess.CalledProcessError as e:
        # e.stdout and e.stderr contain the output from the failed process
        error_detail = f"{e.stdout}\n{e.stderr}".strip()
        raise RuntimeError(f"{failure_message}\n{error_detail}") from e


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
