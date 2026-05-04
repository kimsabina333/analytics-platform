import os
import shutil
import joblib
import numpy as np
import pandas as pd
from itertools import product
from services.telegram import telegram_task_failed_alert, telegram_post_message
from services.slack import slack_post_message, send_slack_image_alert
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from sklearn.preprocessing import OrdinalEncoder
from alert.utils import DataLoader, ModelLoader, make_plot
from services.bigquery import BigQueryBaseHook
import yaml

os.environ["ARVIZ_CACHE_DIR"] = "/tmp/arviz_cache"
slack_test_channel_id = str(Variable.get("slack_alert_channel_id"))

bq_hook = BigQueryBaseHook()



loader = DataLoader(mode='online', sql_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql_templates/sql_templates_recur/'))

encoder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'idata/ordinal_encoder.joblib')
encoder = joblib.load(encoder_path)


q_map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'idata/sr_alert_q_map_recur.yaml')
with open(q_map_path, 'r') as f:
    q_map = yaml.safe_load(f)

features = ['utm_source', 'geo', 'device', 'age', 'gender', 'payment_method', 'card_type', 'mid', 'offer', 'card_brand', 'weekday', 'bank_tier']
cat_map = {
    col: {i: cat for i, cat in enumerate(cats)}
    for col, cats in zip(features, encoder.categories_)
}
coords = {key: list(value.values()) for key, value in cat_map.items()}
coords['feature_dim'] = features
coords['obs_id'] = np.arange(5)


sr_model = ModelLoader(coords=coords, model_path='idata/idata_sr_rec_ts.nc')

offers = ['1Week', '4Week', '12Week', '1Month', '3Month', '1Year']
mids = ['adyen', 'adyen US', 'adyen_us', 'checkout', 'esquire', 'paypal', 'airwallex']
utm_sources = ['facebook', 'google', 'tiktok', 'adq', 'other']
mid_offer_combos = [
    (mid, offer)
    for mid in ['adyen', 'adyen US', 'adyen_us', 'checkout']
    for offer in ['1Week', '4Week', '12Week']
]
mid_pm_combos = [
    (mid, pm)
    for mid in ['adyen', 'adyen US', 'adyen_us', 'checkout']
    for pm in ['card', 'applepay']
]
mid_cb_combos = [
    (mid, cb)
    for mid in ['adyen', 'adyen US', 'adyen_us', 'checkout']
    for cb in ['visa', 'mastercard']
]

def generate_mask_segment(data, date, col, val):
    d = data.loc[date]
    return d[col] == val

def sr_alert(**kwargs):
    execution_date = pd.to_datetime(kwargs['ds'])
    os.makedirs("./plots", exist_ok=True)
    print('execution_date: ', execution_date)

    df = loader(bq_hook)

    data = df.groupby(['date'] + features, as_index=False).agg(
        success=('success', 'sum'), cnt=('cnt', 'sum')
    ).sort_values(['date', 'success'])
    data.set_index('date', inplace=True)
    dates = np.unique(data.index)

    samples = []
    for date in dates:
        daily_data = data.loc[date]
        X_date = encoder.transform(daily_data[features]).astype("int")
        cnt_date = np.array(daily_data.cnt)
        pred = sr_model(cnt_date, X_date)
        samples.append(pred)

    def compute_sr(col, val, min_cnt=50, min_days=3, q=0.05):
        query_str = f"{col}=='{val}'"
        masks = [generate_mask_segment(data, date, col, val).values for date in dates]
        if not any(m.any() for m in masks):
            print(f'No data for {col}={val}, skipping')
            return None, None, None
        valid_idx = [i for i, m in enumerate(masks) if m.any()]
        valid_dates = dates[valid_idx]
        valid_masks = [masks[i] for i in valid_idx]
        valid_samples = [samples[i] for i in valid_idx]
        cnt_segment = data.query(query_str).groupby('date')['cnt'].sum()
        if len(cnt_segment) < min_days:
            print(f'Not enough days for {col}={val}: {len(cnt_segment)} days, skipping')
            return None, None, None
        if cnt_segment.mean() < min_cnt:
            print(f'Not enough cnt for {col}={val}: mean={cnt_segment.mean():.1f}, skipping')
            return None, None, None
        success_dist = np.array([valid_samples[i][:, valid_masks[i]].sum(1) for i in range(len(valid_dates))])
        avg_ci_width = (
            np.quantile(success_dist / cnt_segment.to_numpy().reshape(-1, 1), 0.99, axis=1) -
            np.quantile(success_dist / cnt_segment.to_numpy().reshape(-1, 1), 0.01, axis=1)
        ).mean()
        if avg_ci_width > 0.5:
            print(f'CI too wide for {col}={val}: {avg_ci_width:.2f}, skipping')
            return None, None, None
        sr_dist = success_dist / cnt_segment.to_numpy().reshape(-1, 1)
        sr_fact = data.query(query_str).groupby('date').apply(lambda x: x.success.sum() / x.cnt.sum())
        q_low = np.quantile(sr_dist, q, axis=1)
        last_fact = sr_fact.values[-1]
        last_q_low = q_low[-1]
        is_alert = last_fact < last_q_low
        print(f'{col}={val}: fact={last_fact:.3f}, CI_low={last_q_low:.3f}, alert={is_alert}')
        return sr_dist, sr_fact, is_alert

    def compute_sr_combo(col1, val1, col2, val2, min_cnt=50, min_days=3, q=0.05):
        query_str = f"{col1}=='{val1}' and {col2}=='{val2}'"
        masks = [
            ((data.loc[date][col1] == val1) & (data.loc[date][col2] == val2)).values
            for date in dates
        ]
        if not any(m.any() for m in masks):
            print(f'No data for {col1}={val1} {col2}={val2}, skipping')
            return None, None, None
        valid_idx = [i for i, m in enumerate(masks) if m.any()]
        valid_dates = dates[valid_idx]
        valid_masks = [masks[i] for i in valid_idx]
        valid_samples = [samples[i] for i in valid_idx]
        cnt_segment = data.query(query_str).groupby('date')['cnt'].sum()
        if len(cnt_segment) < min_days:
            print(f'Not enough days for {col1}={val1} {col2}={val2}: {len(cnt_segment)} days')
            return None, None, None
        if cnt_segment.mean() < min_cnt:
            print(f'Not enough cnt for {col1}={val1} {col2}={val2}: mean={cnt_segment.mean():.1f}')
            return None, None, None
        success_dist = np.array([valid_samples[i][:, valid_masks[i]].sum(1) for i in range(len(valid_dates))])
        avg_ci_width = (
            np.quantile(success_dist / cnt_segment.to_numpy().reshape(-1, 1), 0.99, axis=1) -
            np.quantile(success_dist / cnt_segment.to_numpy().reshape(-1, 1), 0.01, axis=1)
        ).mean()
        if avg_ci_width > 0.5:
            print(f'CI too wide for {col1}={val1} {col2}={val2}: {avg_ci_width:.2f}, skipping')
            return None, None, None
        sr_dist = success_dist / cnt_segment.to_numpy().reshape(-1, 1)
        sr_fact = data.query(query_str).groupby('date').apply(lambda x: x.success.sum() / x.cnt.sum())
        q_low = np.quantile(sr_dist, q, axis=1)
        last_fact = sr_fact.values[-1]
        last_q_low = q_low[-1]
        is_alert = last_fact < last_q_low
        print(f'{col1}={val1} {col2}={val2}: fact={last_fact:.3f}, CI_low={last_q_low:.3f}, alert={is_alert}')
        return sr_dist, sr_fact, is_alert

    for val in offers:
        q = q_map.get(f'offer={val}', 0.05)
        sr_dist, sr_fact, is_alert = compute_sr('offer', val, q=q)
        if sr_dist is None:
            continue
        fig_path = f"./plots/sr_offer_{val}.png"
        make_plot(sr_dist, sr_fact, q=q, fig_path=fig_path, save=True)
        if is_alert:
            text = f'\n📉 Recur SR for *offer: {val}* is below CI.'
            send_slack_image_alert(channel_id=slack_test_channel_id, text=text, file_url=fig_path, title=val)
        else:
            print(f'offer={val}: SR normal, no alert')

    for val in mids:
        q = q_map.get(f'mid={val}', 0.05)
        sr_dist, sr_fact, is_alert = compute_sr('mid', val, q=q)
        if sr_dist is None:
            continue
        fig_path = f"./plots/sr_mid_{val}.png"
        make_plot(sr_dist, sr_fact, q=q, fig_path=fig_path, save=True)
        if is_alert:
            text = f'\n📉 Recur SR for *mid: {val}* is below CI.'
            send_slack_image_alert(channel_id=slack_test_channel_id, text=text, file_url=fig_path, title=val)
        else:
            print(f'mid={val}: SR normal, no alert')

    for val in utm_sources:
        q = q_map.get(f'utm_source={val}', 0.05)
        sr_dist, sr_fact, is_alert = compute_sr('utm_source', val, q=q)
        if sr_dist is None:
            continue
        fig_path = f"./plots/sr_utm_{val}.png"
        make_plot(sr_dist, sr_fact, q=q, fig_path=fig_path, save=True)
        if is_alert:
            text = f'\n📉 Recur SR for *utm_source: {val}* is below CI.'
            send_slack_image_alert(channel_id=slack_test_channel_id, text=text, file_url=fig_path, title=val)
        else:
            print(f'utm_source={val}: SR normal, no alert')


    for mid_val, pm_val in mid_pm_combos:
        q = q_map.get(f'mid={mid_val}&payment_method={pm_val}', 0.05)
        sr_dist, sr_fact, is_alert = compute_sr_combo('mid', mid_val, 'payment_method', pm_val, q=q)
        if sr_dist is None:
            continue
        fig_path = f"./plots/sr_{mid_val}_{pm_val}.png".replace(' ', '_')
        make_plot(sr_dist, sr_fact, q=q, fig_path=fig_path, save=True)
        if is_alert:
            text = f'\n📉 Recur SR for *mid: {mid_val} / payment_method: {pm_val}* is below CI.'
            send_slack_image_alert(channel_id=slack_test_channel_id, text=text, file_url=fig_path, title=f'{mid_val}/{pm_val}')
        else:
            print(f'mid={mid_val} payment_method={pm_val}: SR normal, no alert')

    for mid_val, cb_val in mid_cb_combos:
        q = q_map.get(f'mid={mid_val}&card_brand={cb_val}', 0.05)
        sr_dist, sr_fact, is_alert = compute_sr_combo('mid', mid_val, 'card_brand', cb_val, q=q)
        if sr_dist is None:
            continue
        fig_path = f"./plots/sr_{mid_val}_{cb_val}.png".replace(' ', '_')
        make_plot(sr_dist, sr_fact, q=q, fig_path=fig_path, save=True)
        if is_alert:
            text = f'\n📉 Recur SR for *mid: {mid_val} / card_brand: {cb_val}* is below CI.'
            send_slack_image_alert(channel_id=slack_test_channel_id, text=text, file_url=fig_path, title=f'{mid_val}/{cb_val}')
        else:
            print(f'mid={mid_val} card_brand={cb_val}: SR normal, no alert')

    shutil.rmtree("./plots")


with DAG(
    "sr_recur_alert",
    default_args={"retries": 0, "retry_delay": timedelta(seconds=10)},
    description="[Alert] Recurring payment SR",
    schedule="30 23 * * *",
    start_date=datetime(2023, 11, 14),
    on_failure_callback=telegram_task_failed_alert,
    catchup=False,
    tags=["Funnel Conversion", "Alert", "Payment", "Recurring"],
) as dag:
    check_cycle = PythonOperator(
        task_id="sr_recur_alert",
        python_callable=sr_alert,
        provide_context=True,
        dag=dag,
    )
    check_cycle
