import logging

from prefect import flow


logger = logging.getLogger(__name__)


@flow(name="integration_current_labels")
def integration_current_labels() -> None:
    logger.warning(
        "integration_current_labels is blocked — transformation layer for labels not yet complete"
    )


if __name__ == "__main__":
    integration_current_labels()
