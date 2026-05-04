
import os
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from services.bigquery import BigQueryBaseHook
from services.slack import slack_post_message
from services.telegram import telegram_task_failed_alert
from google.cloud.bigquery import LoadJobConfig


import numpy as np
from datetime import datetime, timedelta
from ltv_model.survival_model import SurvivalModel
from ltv_model.utils import LTVForecaster
import joblib

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


dataset_id, table_id = 'analytics_draft', 'ltv_ml_fast'
SLACK_CHANNEL_ID = str(Variable.get('slack_test_channel_id')) #slack_analytics_channel_id

dag_path = os.path.dirname(os.path.abspath(__file__))
directory_path = os.path.join(dag_path, 'ltv_model_weights')
model_path = 'bdw_weights_fast.pth'
config_path = 'config_fast.joblib'

task_type='fast'

bq_hook = BigQueryBaseHook()
model = SurvivalModel()
ltv_forecaster = LTVForecaster(task_type=task_type)



def ltv_upload():
    try:
        df_final = ltv_forecaster.forecast(
        model=model,
        bq_hook=bq_hook,
        directory_path=directory_path,
        model_path=model_path,
        config_path=config_path,
        )
    
        job_config = LoadJobConfig(autodetect=True, write_disposition="WRITE_APPEND")
        job = bq_hook.write_pandas_df_to_table_new(
            dataset_id, table_id, df_final, 
            schema='Without schema', 
            job_config=job_config)
        job.result()

        shape = len(df_final)
        start_date = df_final.subscription_cohort_date.min()
        end_date = df_final.subscription_cohort_date.max()

        text=f'✅ LTV {task_type} uploading was successful. Uploaded {shape} rows since {start_date} till {end_date}'

        slack_post_message(channel_id=SLACK_CHANNEL_ID, text=text)

    except Exception as e:
        logger.error(f"LTV forecast {task_type} failed: {e}")
        slack_post_message(channel_id=SLACK_CHANNEL_ID, text=f"❌ LTV forecast {task_type} failed: {e}")

with DAG('LTVFast',
        default_args={
            'retries': 0,
            'retry_delay': timedelta(seconds=10)},
        description='[ltv_fast_forecast]',
        schedule_interval="0 * * * *",
        start_date=datetime(2023, 11, 14),
        catchup=False,
        on_failure_callback=telegram_task_failed_alert,
        tags=['LTV']) as dag:

    ltv_update = PythonOperator(
        task_id='ltv_fast_upload',
        python_callable=ltv_upload,
        provide_context=True,
    )



