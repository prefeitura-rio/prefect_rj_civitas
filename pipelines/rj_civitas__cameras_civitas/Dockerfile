FROM ghcr.io/prefeitura-rio/prefect_rj_civitas:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_civitas

WORKDIR /opt/prefect/prefect_rj_civitas

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_civitas/
COPY ./src ./src/

COPY ./pipelines/rj_civitas__cameras_civitas ./pipelines/rj_civitas__cameras_civitas/

RUN uv sync --package rj_civitas__cameras_civitas
