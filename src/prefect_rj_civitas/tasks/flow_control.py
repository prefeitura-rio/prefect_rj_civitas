# -*- coding: utf-8 -*-


from __future__ import annotations
from prefect.states import Completed
from prefect import runtime
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowFilter,
    FlowFilterName,
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateName,
    FlowRunFilterStartTime,
)


def skip_if_already_running() -> Completed | None:
    """Returns Completed(Skipped) if another instance of this flow is already running."""
    current_id = runtime.flow_run.id
    current_flow_name = runtime.flow_run.flow_name

    with get_client(sync_client=True) as client:
        current_run = client.read_flow_run(flow_run_id=current_id)
        current_start_time = current_run.start_time or current_run.expected_start_time

        runs = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(name=FlowRunFilterStateName(any_=["Running"])),
                start_time=FlowRunFilterStartTime(before_=current_start_time),
            ),
            flow_filter=FlowFilter(name=FlowFilterName(any_=[current_flow_name])),
        )

    others = [r for r in runs if str(r.id) != current_id]

    if others:
        ids = ", ".join(str(r.id) for r in others)
        return Completed(message=f"Skipped: run(s) {ids} already active.", name="Skipped")
    return None