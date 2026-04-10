FROM apache/superset:6.0.0-dev

USER root

RUN /app/.venv/bin/pip install --no-cache-dir \
    clickhouse-connect \
    psycopg2-binary

COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset
