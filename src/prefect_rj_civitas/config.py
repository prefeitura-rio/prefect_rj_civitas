# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    run_dbt_deployment_name: str = "rj-civitas--run-dbt/rj-civitas--run_dbt"

config = Config()