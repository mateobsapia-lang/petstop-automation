FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash vscode

WORKDIR /workspaces/petstop-automation
COPY requirements.txt /tmp/petstop-requirements.txt
RUN pip install --no-cache-dir -r /tmp/petstop-requirements.txt

USER vscode
