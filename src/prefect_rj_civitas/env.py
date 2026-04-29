# -*- coding: utf-8 -*-
import os
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task


@task
def get_pipeline_secrets_task(pipeline_name: str) -> dict:
    """
    Gets the secrets for a given pipeline from the environment variables.

    Secrets are expected to be in the format <PIPELINE_NAME>__<SECRET_NAME>
    For example:
    - CETRIO_RADAR__URL
    - CETRIO_RADAR__TOKEN
    - DISQUE_DENUNCIA__URL
    - DISQUE_DENUNCIA__TOKEN
    - DISQUE_DENUNCIA__TOKEN
    """
    secrets = {}
    for secret in os.environ:
        if secret.startswith(pipeline_name.upper()):
            secrets[secret] = getenv_or_action(secret)
    return secrets


@task
def verify_secrets_task(secrets: tuple[str, ...]):
    """
    Verifies if the secrets are in the environment variables.
    """
    normalized_secrets = [secret.strip().upper() for secret in secrets]
    missing_secrets = [
        secret
        for secret in normalized_secrets
        if (getenv_or_action(secret, default="", action="ignore")).strip() == ""
    ]

    if missing_secrets:
        raise ValueError(
            f"Missing secrets: {', '.join(sorted(missing_secrets))}"
        )