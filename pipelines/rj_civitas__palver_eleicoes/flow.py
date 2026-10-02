# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake dos dados da Palver (Prefect 3).
"""
import os
from typing import Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task, getenv_or_action
from iplanrio.pipelines_utils.prefect import log, rename_current_flow_run_task
from prefect import flow
from prefect_rj_civitas import (
    skip_if_already_running,
    verify_secrets_task,
)

from pipelines.rj_civitas__palver_eleicoes.tasks import (
    get_palver_token_task,
    fetch_messages_task,
    load_to_table_task,
    resolve_start_date_task,
    resolve_incremental_date_task,
    clean_text_task,
    get_dd_data
)

from prefect_rj_civitas import (
    send_elections_context_alerts
)


@flow(log_prints=True)
def rj_civitas__palver_eleicoes(
    project_id: str = "rj-civitas",
    dataset_id: str = "alerta_contexto_eleicoes",
    sources: list[Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter", "telegram"]] = ["whatsapp", "news", "telegram"],
    get_disque_denuncia: bool = True,
    docs_per_page: int = 100,
    incremental: bool = True,
    start_date: str | None = "2026-10-01 00:00:00",
    end_date: str | None = None,
    minutes_offset: int = 1440,
    query: str = "(eleição OR eleições) AND ('fraude' OR 'urna fraudada' OR 'sabotagem' OR 'Comando Vermelho' OR CV OR TCP OR 'Terceiro Comando' OR traficante~ OR miliciano~)",
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    llm_model: str = "gemini-2.5-flash",
    mode: Literal["dev", "prod", "staging"] = "staging",
    required_secrets: tuple[str, ...] = (
        "PALVER_BASE_URL",
        "PALVER_USERNAME",
        "PALVER_PASSWORD",
        "REDIS_HOST",
        "REDIS_PASSWORD",
        "SLACK_ALERT_CHANNEL",
        "SLACK_ALERT_TOKEN"
    )
):
    rename_current_flow_run_task(new_name=f"{write_disposition}_{dataset_id}_messages-{mode}")

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    palver_email = getenv_or_action("PALVER_USERNAME", action="raise")
    palver_password = getenv_or_action("PALVER_PASSWORD", action="raise")
    redis_password = getenv_or_action("REDIS_PASSWORD", action="raise")
    palver_token = get_palver_token_task(palver_email=palver_email, palver_password=palver_password, redis_password=redis_password)

    resolved_start_date = resolve_start_date_task(start_date, minutes_offset)

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    llm_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    slack_channel = os.getenv("SLACK_ALERT_CHANNEL")
    slack_token = os.getenv("SLACK_ALERT_TOKEN")
    for source in sources:
        table_id = f"palver_eleicoes_{source.replace('.', '_')}_messages"

        if incremental:
            incremental_date = resolve_incremental_date_task(
                project_id=project_id,
                dataset_id=f"{dataset_id}",
                table_id=table_id
            )
            if incremental_date:
                resolved_start_date = incremental_date

        data = fetch_messages_task(
            start_date=resolved_start_date,
            end_date=end_date,
            docs_per_page=docs_per_page,
            source=source,
            query=query,
            palver_token=palver_token
        )

        if not data:
            log(f"No data from {source} returned by the API.")
            continue

        data = clean_text_task(source=source, data=data)

        load_to_table_task(
            project_id=project_id,
            dataset_id= dataset_id,
            table_id=table_id,
            source=source,
            data=data,
            write_disposition=write_disposition
        )

        if source in ("whatsapp", "telegram", "news"):
            send_elections_context_alerts(
                source = source,
                target_project_id=project_id,
                target_dataset_id=dataset_id,
                target_table_id=f"alerta_eleicoes_{source}",
                data=data,
                llm_model="gemini-2.5-flash",
                llm_credentials=llm_credentials,
                slack_api_url="https://slack.com/api/chat.postMessage",
                slack_bot_token=slack_token,
                slack_channel_id=slack_channel
            )

    if not get_disque_denuncia:
        return

    target_dd_table = "alerta_eleicoes_disque_denuncia"
    dd_data = get_dd_data(project_id, target_dd_table, start_date)

    if not dd_data:
        log("Não foi retornado nenhum registro do Disque Denúncia")
        return

    send_elections_context_alerts(
        source = "disque_denuncia",
        target_project_id=project_id,
        target_dataset_id=dataset_id,
        target_table_id=target_dd_table,
        data=dd_data,
        llm_model="gemini-2.5-flash",
        llm_credentials=llm_credentials,
        slack_api_url="https://slack.com/api/chat.postMessage",
        slack_bot_token=slack_token,
        slack_channel_id=slack_channel
    )

