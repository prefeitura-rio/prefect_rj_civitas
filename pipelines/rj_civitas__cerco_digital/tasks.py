# -*- coding: utf-8 -*-
from typing import Literal
from prefect import task
from datetime import datetime, timezone, timedelta


@task
def add_default_start_date_var_to_dbt_flags(
    flag: str | None = None,
    period: Literal["last_hour", "last_day", "seven_days_ago", "thirty_days_ago"] = "last_day",
) -> str:
    """
    Append `--vars '{"start_date": "..."}'` for dbt (optional extra `flag` before it).

    period (UTC):
        last_hour — start of the hour of (now - 1 hour), "YYYY-MM-DD HH:00:00"
        last_day — midnight of the calendar day of (now - 1 day), "YYYY-MM-DD"
        seven_days_ago — midnight of the calendar day of (now - 7 days)
        thirty_days_ago — midnight of the calendar day of (now - 30 days)
    """
    if period == "last_hour":
        now = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        start_date = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")
    elif period == "last_day":
        now = datetime.now(tz=timezone.utc) - timedelta(days=1)
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    elif period == "seven_days_ago":
        now = datetime.now(tz=timezone.utc) - timedelta(days=7)
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    elif period == "thirty_days_ago":
        now = datetime.now(tz=timezone.utc) - timedelta(days=30)
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Invalid period: {period}")

    if flag:
        return f"{flag} --vars '{{\"start_date\": \"{start_date}\"}}'"
    else:
        return f"--vars '{{\"start_date\": \"{start_date}\"}}'"
