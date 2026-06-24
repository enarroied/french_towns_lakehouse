from flows.shared import log
from flows_integration.integration.integration_current_dim_geography import (
    integration_current_dim_geography,
)
from flows_integration.integration.integration_current_fact_demographics import (
    integration_current_fact_demographics,
)
from flows_integration.integration.integration_current_fact_unemployment import (
    integration_current_fact_unemployment,
)
from flows_integration.integration.integration_current_labels import (
    integration_current_labels,
)
from prefect import flow


@flow(name="integration_pipeline")
def integration_pipeline() -> None:
    """Run all integration pipelines to test them end-to-end."""
    log("Starting Integration Pipeline")

    log("=== GEOGRAPHY ===")
    integration_current_dim_geography()

    log("=== DEMOGRAPHICS ===")
    integration_current_fact_demographics()
    integration_current_fact_unemployment()

    log("=== LABELS ===")
    integration_current_labels()

    log("Integration Pipeline complete")


if __name__ == "__main__":
    integration_pipeline()
