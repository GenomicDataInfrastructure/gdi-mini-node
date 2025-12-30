FROM python:3.14-slim-bookworm
ENV TZ=UTC

# System update:
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Install Poetry for loading dependencies:
RUN VENV_PATH=/opt/poetry; mkdir -p $VENV_PATH \
    && python3 -m venv $VENV_PATH \
    && $VENV_PATH/bin/pip install -U pip setuptools \
    && $VENV_PATH/bin/pip install poetry \
    && ln -s $VENV_PATH/bin/poetry /usr/local/bin/poetry

# Run Poetry without virtual environment to make fastapi directly accessible:
ENV POETRY_VIRTUALENVS_CREATE=false

# Deploy application with dependencies:
RUN mkdir -p /app/data && chown -R nobody:nogroup /app/
COPY --chown=nobody:nogroup pyproject.toml poetry.lock uvicorn-log-config.yaml /app/
RUN cd /app && poetry install --no-root --compile
COPY --chown=nobody:nogroup mini_node /app/mini_node

# RUN AS nobody:nogroup
USER 65534:65534
EXPOSE 8008/tcp
WORKDIR /app

# Expected location for configuration files:
VOLUME /app/config

# Expected location for reading and storing Parquet files:
VOLUME /app/data

# Disable coloured logs:
ENV NO_COLOR=1

# This tells uvicorn to trust 'X-Forwarded-*' headers from any IP address:
ENV FORWARDED_ALLOW_IPS="*"

CMD ["uvicorn", "mini_node:app", \
  "--host", "0.0.0.0", "--port", "8000", \
  "--log-config", "uvicorn-log-config.yaml", \
  "--proxy-headers", "--no-server-header", "--no-use-colors"]
