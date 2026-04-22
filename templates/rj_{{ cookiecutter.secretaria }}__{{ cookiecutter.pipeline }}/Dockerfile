FROM ghcr.io/prefeitura-rio/prefect_rj_civitas:latest

LABEL org.opencontainers.image.source=https://github.com/prefeitura-rio/prefect_rj_civitas

WORKDIR /opt/prefect/prefect_rj_civitas

COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_civitas/
COPY ./src ./src/

COPY ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }} ./pipelines/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/

RUN uv sync --package rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}
