FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ libgomp1 git \
    && rm -rf /var/lib/apt/lists/*

COPY webapp/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Vendor: ML code with correct import structure
RUN mkdir -p /app/vendor/idata /app/vendor/sql_templates /app/vendor/alert
COPY utils.py /app/vendor/utils.py
COPY banks_mapping.py /app/vendor/banks_mapping.py
COPY banks_mapping.py /app/vendor/alert/banks_mapping.py
RUN touch /app/vendor/alert/__init__.py

# Data files (rename to remove " (1)" suffix)
COPY ["idata_sr_ts (1).nc", "/app/vendor/idata/idata_sr_ts.nc"]
COPY ["ordinal_encoder (1).joblib", "/app/vendor/idata/ordinal_encoder.joblib"]
COPY matched_groups_final.csv /app/vendor/idata/matched_groups_final.csv
COPY banks_mapping.json /app/vendor/banks_mapping.json
COPY "sr_alert_q_map.yaml" /app/vendor/idata/sr_alert_q_map.yaml

# SQL templates
COPY ["sql_config (1).yaml", "/app/vendor/sql_templates/sql_config.yaml"]
COPY main.sql /app/vendor/sql_templates/main.sql
COPY funnel.sql /app/vendor/sql_templates/funnel.sql
COPY payments.sql /app/vendor/sql_templates/payments.sql
COPY app.sql /app/vendor/sql_templates/app.sql

# Backend + frontend
COPY webapp/backend/ /app/backend/
COPY webapp/test_ui.html /app/test_ui.html

ENV PYTHONPATH=/app
ENV MPLBACKEND=Agg
ENV ARVIZ_CACHE_DIR=/tmp/arviz_cache

# PORT is injected by Railway; fall back to 8000 locally
CMD ["sh", "-c", "uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --timeout-keep-alive 120"]
