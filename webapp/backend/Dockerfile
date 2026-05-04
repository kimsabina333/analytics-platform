FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ libgomp1 git \
    && rm -rf /var/lib/apt/lists/*

COPY webapp/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Vendor: ML code with correct import structure
RUN mkdir -p /app/vendor/idata /app/vendor/sql_templates /app/vendor/sql_templates_recur /app/vendor/alert
COPY utils.py /app/vendor/utils.py
COPY banks_mapping.py /app/vendor/banks_mapping.py
COPY banks_mapping.py /app/vendor/alert/banks_mapping.py
RUN touch /app/vendor/alert/__init__.py

# Data files with spaces/parens in names — COPY via glob to temp dirs, then rename
COPY idata_sr_ts*.nc      /tmp/root_nc/
COPY ordinal_encoder*.joblib /tmp/root_enc/
COPY sql_config*.yaml     /tmp/root_cfg/
RUN cp "/tmp/root_nc/idata_sr_ts (1).nc"          /app/vendor/idata/idata_sr_ts.nc \
 && cp "/tmp/root_enc/ordinal_encoder (1).joblib"  /app/vendor/idata/ordinal_encoder.joblib \
 && cp "/tmp/root_cfg/sql_config (1).yaml"         /app/vendor/sql_templates/sql_config.yaml

COPY matched_groups_final.csv /app/vendor/idata/matched_groups_final.csv
COPY banks_mapping.json       /app/vendor/banks_mapping.json
COPY sr_alert_q_map.yaml      /app/vendor/idata/sr_alert_q_map.yaml

# SQL templates (no spaces in filenames)
COPY main.sql     /app/vendor/sql_templates/main.sql
COPY funnel.sql   /app/vendor/sql_templates/funnel.sql
COPY payments.sql /app/vendor/sql_templates/payments.sql
COPY app.sql      /app/vendor/sql_templates/app.sql

# Recurring SR model — directory name has a space; use ? wildcard to match it
COPY sr?recurring/ /tmp/sr_rec/
RUN cp "/tmp/sr_rec/idata_sr_rec_ts (1).nc"          /app/vendor/idata/idata_sr_rec_ts.nc \
 && cp "/tmp/sr_rec/sr_alert_q_map_recur (1).yaml"   /app/vendor/idata/sr_alert_q_map_recur.yaml \
 && cp /tmp/sr_rec/main.sql                           /app/vendor/sql_templates_recur/main.sql \
 && cp /tmp/sr_rec/funnel.sql                         /app/vendor/sql_templates_recur/funnel.sql \
 && cp /tmp/sr_rec/app.sql                            /app/vendor/sql_templates_recur/app.sql \
 && cp /tmp/sr_rec/firsts.sql                         /app/vendor/sql_templates_recur/firsts.sql \
 && cp "/tmp/sr_rec/payments (1).sql"                 /app/vendor/sql_templates_recur/payments.sql \
 && cp "/tmp/sr_rec/sql_config (2).yaml"              /app/vendor/sql_templates_recur/sql_config.yaml

# Backend + frontend
COPY webapp/backend/ /app/backend/
COPY webapp/test_ui.html /app/test_ui.html

ENV PYTHONPATH=/app
ENV MPLBACKEND=Agg
ENV ARVIZ_CACHE_DIR=/tmp/arviz_cache

# PORT is injected by Railway; fall back to 8000 locally
CMD ["sh", "-c", "uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --timeout-keep-alive 120"]
