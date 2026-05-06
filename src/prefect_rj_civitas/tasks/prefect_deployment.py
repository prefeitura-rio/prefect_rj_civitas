# -*- coding: utf-8 -*-
from prefect import flow, task
from prefect.deployments import run_deployment
from prefect.flows import FlowRun
from collections.abc import Coroutine
from typing import Any


@task
def run_deployment_task(
    name: str,
    parameters: dict,
    timeout: int | None = None,
    as_subflow: bool = False,
    **kw,
) -> (FlowRun | Coroutine[Any, Any, FlowRun]):
    """
    Run a Prefect deployment as a task

    Args:
        name (str): The name of the deployment to run
        parameters (dict): The parameters to pass to the deployment
        timeout (int): The timeout to pass to the run_deployment function.
        as_subflow (bool): Whether to run the deployment as a subflow
        **kw: Additional keyword arguments to pass to the run_deployment function

        Examples:
            - To wait for deployment completion, set timeout to None.
            - To run in background (fire-and-forget), set timeout to 0.
            - To run as a subflow, set as_subflow to True.
            - To pass additional keyword arguments, pass them as **kw.

    """
    return run_deployment(
        name=name,
        parameters=parameters,
        timeout=timeout,
        as_subflow=as_subflow,
        **kw,
    )