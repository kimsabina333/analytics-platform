
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from services.bigquery import BigQueryBaseHook
from services.telegram import telegram_task_failed_alert
from google.cloud.bigquery import LoadJobConfig


import numpy as np
from datetime import datetime, timedelta
from ltv_model.survival_model import SurvivalModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# from services.bigquery import BigQueryBaseHook отдельно прописать загрузку

dataset_id, table_id = 'analytics_draft', 'ltv_ml_approach'
bq_hook = BigQueryBaseHook()
directory_path = 'ltv_model/ltv_model_weights'
pop_countries = ['US', 'AU', 'GB', 'AE', 'CA', 'NZ', 'SG']
feature_cols = [
    'offer', 'channel', 'utm_source', 'gender',
    'age', 'payment_method', 'first_amount',
    'geo', 'upsell_amount', 'unsub'
    ]


def process_gender(x):
    if x.gender in ('Male →', 'Female →'):
        return x.gender.split(' ')[0].lower()
    return x.gender.lower()

def process_age(x):
    if x.age in ('45', '45+'):
        return '45+'
    elif x.age == '36-45':
        return '35-44'
    elif x.age == '18-25':
        return '18-24'
    elif x.age == '26-35':
        return '25-34'
    return x.age

def process_channel(x):
    if x.mid in ['esquire', 'adyen_us']: return np.nan
    if x.channel == 'primer':
        return x.mid
    return x.channel


def process_geo(x):
    return x.geo_country if x.geo_country in pop_countries else x.geo

def process_utm_source(x):
    return x.utm_source if x.utm_source!='adq' else np.nan

def load_data(ds):
    pass

def ltv_forecast(**kwargs):
    
    df = load_data(kwargs['ds'])

    df = df.groupby('customer_account_id', as_index=False).max()

    df.set_index('customer_account_id', inplace=True)

    df['age'] = df.apply(process_age, axis=1)
    df['gender'] = df.apply(process_gender, axis=1)
    df['channel'] = df.apply(process_channel, axis=1)
    df['utm_source'] = df.apply(process_utm_source, axis=1)

    # log data with nan values to prevent errors
    df.dropna(inplace=True)

    X = df[feature_cols]

    pc_mask = X.offer.apply(lambda x: 6 if x!='12Week' else 2).to_numpy()

    model = SurvivalModel()
    model.load(directory_path=directory_path)

    df_final = model.ltv_calc(X, pc_mask)
    df_final['arppu'] = df_final['first_amount'] + df_final['upsell_amount']
    df_final['ltv'] = df_final['arppu'] + df_final['ltv_recurring']
    df_final = df_final.merge(
        df[['subscription_cohort_date', 'churned']], 
        on='customer_account_id', 
        how='left',
        )
    
    job_config = LoadJobConfig(autodetect=True, write_disposition="WRITE_APPEND")
    job = bq_hook.write_pandas_df_to_table_new(
        dataset_id, table_id, df_final, 
        schema='Without schema', 
        job_config=job_config)
    
# with DAG('LTV_Pipeline',
#      default_args={
#          'retries': 0,
#          'retry_delay': timedelta(seconds=10)},
#      description='[ltv forecast]',
#      schedule_interval="0 12 * * *",
#      start_date=datetime(2023, 11, 14),
#      catchup=False,
#      on_failure_callback=telegram_task_failed_alert,
#      tags=['LTV']) as dag:

#     ltv_update = PythonOperator(
#         task_id='ltv_update',
#         python_callable=ltv_forecast,
#         provide_context=True,
#     )



