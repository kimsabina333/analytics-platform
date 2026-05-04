import asyncio
import time
from typing import Optional

import pandas as pd

from backend.services.db_service import DBService

CACHE_TTL = 3600

ROI_SQL = r"""
with
no_dup as (
  select distinct (order_id), *
  from `payments.all_payments_prod`
  where status = 'settled'
),

arppu as (
  select distinct
    customer_account_id,
    sum(
      case when payment_type = 'upsell' then (0.83*amount*coalesce(exchange_rate, 1))/100
      when subscription_id in ('33', '34', '35') then (0.87*amount*coalesce(exchange_rate, 1))/100
      else (0.85*amount*coalesce(exchange_rate, 1))/100
      end) as arppu,
    sum(
      case when payment_type = 'upsell' then (0.83*amount*coalesce(exchange_rate, 1))/100 end)
    as upsell_sum,
    max(case when payment_type = 'upsell' and date(timestamp_micros(created_at)) >= '2025-09-03' and paid_count = -3 then 1 else 0 end) is_new_upsell
  from no_dup p
  left join `analytics_draft.exchange_rate` ex
  on p.currency = ex.currency
  and p.date = ex.date
  where 1=1
    and (payment_type = 'first' or payment_type = 'upsell')
  group by 1
),

fa as (
  select
    frt.user_id,
    event_name,
    arppu.arppu,
    arppu.upsell_sum,
    is_new_upsell,
    timestamp_add(timestamp, interval 300 minute) timestamp_new,
    case
      when json_value(event_metadata, '$.subscription_id') in ('2', '12', '15', '18', '21', '24', '27', '30') then '1Week'
      when json_value(event_metadata, '$.subscription_id') in ('3', '13', '16', '19', '22', '25', '28', '31') then '4Week'
      when json_value(event_metadata, '$.subscription_id') in ('4', '14', '17', '20', '23', '26', '29', '32') then '12Week'
      when json_value(event_metadata, '$.subscription_id') = '33' then '1Month'
      when json_value(event_metadata, '$.subscription_id') = '34' then '3Month'
      when json_value(event_metadata, '$.subscription_id') = '35' then '1Year'
    else '1Week'
    end as offer,
    case
      when coalesce(country, country_code) in ("AE", "AT", "AU", "BH", "BN", "CA", "CZ", "DE", "DK", "ES", "FI", "FR",
      "GB", "HK", "IE", "IL", "IT", "JP", "KR", "NL", "NO", "PT", "QA", "SA", "SE", "SG", "SI", "US", "NZ")
      then 'T1'
      else 'WW'
    end as geo,
    case
      when json_value(event_metadata, '$.payment_method') in ('paypal', 'paypal-vault') then 'applepay'
      else coalesce(json_value(event_metadata, '$.payment_method'), 'card')
    end as payment_method,
    json_value(event_metadata, '$.age') age,
    case
      when json_value(event_metadata, '$.utm_source') in ('fb_bio', 'fb_page', 'fb_post', 'facebook', 'insta_bio', 'instagram') then 'facebook'
      when json_value(event_metadata, '$.utm_source') = 'google' then 'google'
      when lower(json_value(event_metadata, '$.utm_source')) = 'tiktok' then 'tiktok'
      when json_value(event_metadata, '$.utm_source') = 'landing_framer' then 'landing_framer'
      else 'other'
    end as utm_source,
    json_value(event_metadata, '$.em_source') em_source,
    json_value(frt.event_metadata, '$.utm_campaign') as user_campaign_id
  from `events.funnel-raw-table` frt
  left join arppu
    on frt.user_id = arppu.customer_account_id
  where 1=1
    and json_value(event_metadata, '$.utm_source') not in ('unionapps')
    and event_name = 'pr_funnel_subscribe'
    and date(timestamp_add(timestamp, interval 300 minute)) between current_date() - 15 and current_date()
),

ltv_new as (
  select geo, utm_source, payment_method, offer, ltv_net
  from `analytics_draft.ltv_new_approach`
),

ltv_ml as (
  select customer_account_id, ltv_recurring*0.8 as ltv_net
  from `analytics_draft.ltv_ml_approach`
),

ltv_ml_fast as (
  select customer_account_id, ltv_recurring*0.85 as ltv_net
  from `analytics_draft.ltv_ml_fast`
),

fa_ltv as (
  select
    fa.*,
    coalesce(ltv.ltv_final, 20) as ltv_exp,
    case when is_new_upsell = 1 then 0 else coalesce(ltv_new.ltv_net, 20) end as ltv_new,
    case when is_new_upsell = 1 then 0 else coalesce(ltv_ml.ltv_net, 20) end as ltv_ml,
    case when is_new_upsell = 1 then 0 else coalesce(ltv_ml_fast.ltv_net, 20) end as ltv_ml_fast,
  from fa
  left join `analytics_draft.ltv_exp` ltv
    on fa.geo = ltv.geo
    and fa.offer = ltv.offer
    and fa.payment_method = ltv.payment_method
    and fa.utm_source = ltv.utm_source
  left join ltv_new
    on fa.geo = ltv_new.geo
    and fa.offer = ltv_new.offer
    and fa.payment_method = ltv_new.payment_method
    and fa.utm_source = ltv_new.utm_source
  left join ltv_ml
    on fa.user_id = ltv_ml.customer_account_id
  left join ltv_ml_fast
    on fa.user_id = ltv_ml_fast.customer_account_id
),

ltv_general as (
  select
    date(timestamp_new) as date,
    utm_source,
    count(distinct user_id) as purch_count,
    sum(case when offer = '1Week' then 1 else 0 end) as week1_purch,
    sum(case when offer = '4Week' then 1 else 0 end) as week4_purch,
    sum(case when offer = '12Week' then 1 else 0 end) as week12_purch,
    sum(case when offer = '1Month' then 1 else 0 end) as month1_purch,
    sum(case when offer = '3Month' then 1 else 0 end) as month3_purch,
    sum(case when offer = '1Year' then 1 else 0 end) as year1_purch,
    sum(case when age = '18-24' then 1 else 0 end) as age18_24_purch,
    sum(case when geo = 'WW' then 1 else 0 end) as ww_purch,
    sum(ltv_exp) as ltv_exp,
    sum(ltv_new) as ltv_new,
    sum(ltv_ml) as ltv_ml,
    sum(ltv_ml_fast) as ltv_ml_fast,
    sum(arppu) as arppu,
    sum(upsell_sum) as upsell_sum
  from fa_ltv
  group by 1, 2
),

cab_fb as (
  select
    date(bas.date_start) date,
    'facebook' as utm_source,
    coalesce(sum(impressions), 0) as impressions,
    coalesce(sum(spend), 0) as spend,
    coalesce(sum(inline_link_clicks), 0) as click,
    coalesce(sum(pixel_initiate_checkout)) cab_ttp,
    coalesce(sum(pixel_purchases)) cab_purch,
  from `facebook_api.spend_by_age` bas
  where date(bas.date_start) between current_date() - 15 and current_date()
  group by 1, 2
),

cab_google as (
  select
    date,
    'google' as utm_source,
    coalesce(sum(impressions), 0) as impressions,
    coalesce(sum(cost_micros) / 1000000, 0) as spend,
    coalesce(sum(clicks), 0) as click,
    0 as cab_ttp,
    coalesce(sum(conversions)) as cab_purch,
  from `google_api.google_campaigns`
  where date between current_date() - 15 and current_date()
  group by 1, 2
),

cab_tiktok as (
  select
    date_start,
    'tiktok' as utm_source,
    coalesce(sum(impressions), 0) as impressions,
    coalesce(sum(spend), 0) as spend,
    coalesce(sum(clicks), 0) as click,
    0 as cab_ttp,
    0 as cab_purch
  from `tiktok_api.spend_by_campaign`
  where date_start between current_date() - 15 and current_date()
  group by 1, 2
),

funnel_no_sp as (
  select distinct
    case when event_name in ('pr_funnel_landing_page_view', 'pr_funnel_click', 'pr_funnel_email_page_view') then device_id else user_id end as user_id,
    case
      when json_value(event_metadata, '$.utm_source') in ('fb_bio', 'fb_page', 'fb_post', 'facebook', 'insta_bio', 'instagram') then 'facebook'
      when json_value(event_metadata, '$.utm_source') = 'google' then 'google'
      when lower(json_value(event_metadata, '$.utm_source')) = 'tiktok' then 'tiktok'
      when json_value(event_metadata, '$.utm_source') = 'landing_framer' then 'landing_framer'
      else 'other'
    end as utm_source,
    json_value(event_metadata, '$.em_source') em_source,
    date(timestamp_add(timestamp, interval 300 minute)) date,
    event_name
  from `events.funnel-raw-table`
  where event_name in ('pr_funnel_landing_page_view', 'pr_funnel_click', 'pr_funnel_email_page_view', 'pr_funnel_email_submit', 'pr_funnel_paywall_view', 'pr_funnel_paywall_purchase_click', 'pr_funnel_subscribe')
    and date(timestamp_add(timestamp, interval 300 minute)) between current_date()-15 and current_date()
    and json_value(event_metadata, '$.utm_source') not in ('unionapps', 'mailerlite')
    and ip not like "173.252%"
    and ip not like "69.171%"
    and ip not like "66.220%"
    and ip not like "31.13%"
    and (user_agent not like '%AdsBot%' or user_agent is null)
    and (user_agent not like '%facebookexternalhit%' or user_agent is null)
    and (user_agent not like '%Google-Read-Aloud%' or user_agent is null)
),

chse as (
  select
    sub.user_id,
    json_value(ttp.event_metadata, '$.chase') chase
  from `events.funnel-raw-table` sub
  inner join `events.funnel-raw-table` ttp
    on sub.user_id = ttp.user_id
    and ttp.event_name = 'pr_funnel_paywall_purchase_click'
    and date(ttp.timestamp) >= current_date()-16
  where sub.event_name = 'pr_funnel_subscribe'
    and date(timestamp_add(sub.timestamp, interval 300 minute)) between current_date()-15 and current_date()
),

funnel as (
  select f.*, chase
  from funnel_no_sp f
  left join chse c
    on c.user_id = case when event_name = 'pr_funnel_subscribe' then f.user_id end
  union all
  select
    sp.user_id,
    case
      when json_value(event_metadata, '$.utm_source') in ('fb_bio', 'fb_page', 'fb_post', 'facebook', 'insta_bio', 'instagram') then 'facebook'
      when json_value(event_metadata, '$.utm_source') = 'google' then 'google'
      when lower(json_value(event_metadata, '$.utm_source')) = 'tiktok' then 'tiktok'
      when json_value(event_metadata, '$.utm_source') = 'landing_framer' then 'landing_framer'
      else 'other'
    end as utm_source,
    json_value(event_metadata, '$.em_source') em_source,
    date(timestamp_add(timestamp, interval 300 minute)) date,
    sp.event_name,
    json_value(event_metadata, '$.chase') chase
  from `events.funnel-raw-table` sp
  inner join funnel_no_sp f
    on f.user_id = sp.user_id
    and f.event_name = 'pr_funnel_email_submit'
    and f.date = date(timestamp_add(timestamp, interval 300 minute))
  where sp.event_name = 'pr_funnel_selling_page_view'
    and date(timestamp_add(timestamp, interval 300 minute)) between current_date()-15 and current_date()
    and json_value(event_metadata, '$.utm_source') not in ('unionapps', 'mailerlite')
    and ip not like "173.252%"
    and ip not like "69.171%"
    and ip not like "66.220%"
    and ip not like "31.13%"
    and (user_agent not like '%AdsBot%' or user_agent is null)
    and (user_agent not like '%facebookexternalhit%' or user_agent is null)
    and (user_agent not like '%Google-Read-Aloud%' or user_agent is null)
),

upsell as (
  select date, utm_source, uf.user_id
  from `events.funnel-raw-table` uf
  inner join funnel f on f.user_id = uf.user_id and f.event_name = 'pr_funnel_subscribe'
  where uf.event_name = 'pr_webapp_upsell_successful_purchase'
  union all
  select date, utm_source, ua.user_id
  from `events.app-raw-table` ua
  inner join funnel f on f.user_id = ua.user_id and f.event_name = 'pr_funnel_subscribe'
  where ua.event_name = 'pr_webapp_upsell_successful_purchase'
  union all
  select date, utm_source, u2.user_id
  from `events.app-raw-table` u2
  inner join funnel f on f.user_id = u2.user_id and f.event_name = 'pr_funnel_subscribe'
  where u2.event_name = 'pr_webapp_upsell_page2_successful_purchase'
),

upsell_cnt as (
  select date, utm_source, count(distinct user_id) upsell_cnt
  from upsell
  group by 1, 2
),

funnel_cnt as (
  select
    f.date,
    f.utm_source,
    count(distinct case when event_name = 'pr_funnel_landing_page_view' then user_id end) lv_cnt,
    count(distinct case when event_name = 'pr_funnel_click' then user_id end) sq_cnt,
    count(distinct case when event_name = 'pr_funnel_email_page_view' then user_id end) emv_cnt,
    count(distinct case when event_name = 'pr_funnel_email_submit' then user_id end) es_cnt,
    count(distinct case when event_name = 'pr_funnel_selling_page_view' then user_id end) sp_cnt,
    count(distinct case when event_name = 'pr_funnel_paywall_view' then user_id end) pw_cnt,
    count(distinct case when event_name = 'pr_funnel_paywall_purchase_click' then user_id end) ttp_cnt,
    count(distinct case when event_name = 'pr_funnel_subscribe' then user_id end) sub_cnt,
    count(distinct case when event_name = 'pr_funnel_subscribe' and chase = 'true' then user_id end) chase_sub,
    count(distinct case when event_name = 'pr_funnel_subscribe' and (em_source is not null and em_source != '') then user_id end) em_sub
  from funnel f
  group by 1, 2
),

fun_x_up as (
  select f.*, upsell_cnt
  from funnel_cnt f
  left join upsell_cnt u
    on f.date = u.date
    and f.utm_source = u.utm_source
),

cab_all as (
  select * from cab_fb
  union all select * from cab_google
  union all select * from cab_tiktok
)

select
  coalesce(ca.date, lg.date) date,
  coalesce(ca.impressions, 0) impressions,
  coalesce(ca.click, 0) click,
  coalesce(ca.spend, 0) spend,
  coalesce(ca.utm_source, lg.utm_source) utm_source,
  coalesce(ww_purch, 0) ww_purch,
  coalesce(week1_purch, 0) week1_purch,
  coalesce(week4_purch, 0) week4_purch,
  coalesce(week12_purch, 0) week12_purch,
  coalesce(month1_purch, 0) month1_purch,
  coalesce(month3_purch, 0) month3_purch,
  coalesce(year1_purch, 0) year1_purch,
  coalesce(age18_24_purch, 0) age18_24_purch,
  coalesce(purch_count, 0) purch_count,
  coalesce(lg.ltv_exp, 0) ltv_exp,
  coalesce(lg.ltv_new, 0) ltv_new,
  coalesce(lg.arppu, 0) arppu,
  coalesce(lg.upsell_sum, 0) upsell_sum,
  coalesce(lg.ltv_new+lg.arppu, 0) ltv_neww,
  coalesce(lg.ltv_ml+lg.arppu, 0) ltv_ml,
  coalesce(lg.ltv_ml_fast+lg.arppu, 0) ltv_ml_fast,
  coalesce(lv_cnt, 0) lv_cnt,
  coalesce(sq_cnt, 0) sq_cnt,
  coalesce(emv_cnt, 0) emv_cnt,
  coalesce(es_cnt, 0) es_cnt,
  coalesce(sp_cnt, 0) sp_cnt,
  coalesce(pw_cnt, 0) pw_cnt,
  coalesce(ttp_cnt, 0) ttp_cnt,
  coalesce(sub_cnt, 0) sub_cnt,
  coalesce(chase_sub, 0) chase_cnt,
  coalesce(em_sub, 0) em_sub,
  coalesce(upsell_cnt, 0) upsell_cnt,
  coalesce(cab_purch, 0) cab_purch,
  coalesce(cab_ttp, 0) cab_ttp
from cab_all ca
full join ltv_general lg
  on ca.date = lg.date
  and ca.utm_source = lg.utm_source
left join fun_x_up f
  on ca.date = f.date
  and ca.utm_source = f.utm_source
"""


class MarketingService:
    def __init__(self, bq_client, db_svc: Optional[DBService] = None):
        self.bq_client = bq_client
        self.db = db_svc
        self._roi_cache: pd.DataFrame | None = None
        self._roi_cache_time = 0.0
        self._roi_lock = asyncio.Lock()

    def _cache_get_df(self, key: str) -> pd.DataFrame | None:
        if self.db is None:
            return None
        cached = self.db.cache_get(key)
        if not cached:
            return None
        return pd.read_json(cached, orient="records")

    def _cache_set_df(self, key: str, df: pd.DataFrame) -> None:
        if self.db is not None:
            self.db.cache_set(key, df.to_json(orient="records", date_format="iso"))

    async def _load_roi(self) -> pd.DataFrame:
        async with self._roi_lock:
            if self._roi_cache is not None and time.time() - self._roi_cache_time < CACHE_TTL:
                return self._roi_cache
            cached = self._cache_get_df("marketing:roi:v1")
            if cached is not None:
                cached["date"] = pd.to_datetime(cached["date"]).dt.strftime("%Y-%m-%d")
                self._roi_cache = self._add_roi_metrics(cached)
                self._roi_cache_time = time.time()
                return self._roi_cache
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: self.bq_client.query(ROI_SQL).to_dataframe()
            )
            raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")
            self._cache_set_df("marketing:roi:v1", raw)
            self._roi_cache = self._add_roi_metrics(raw)
            self._roi_cache_time = time.time()
            return self._roi_cache

    def _add_roi_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        spend = df["spend"].replace(0, float("nan"))
        df["roi_new"] = (df["ltv_neww"] - df["spend"]) / spend
        df["roi_ml"] = (df["ltv_ml"] - df["spend"]) / spend
        df["roi_ml_fast"] = (df["ltv_ml_fast"] - df["spend"]) / spend
        df["cac"] = df["spend"] / df["purch_count"].replace(0, float("nan"))
        df["gp"] = df["ltv_neww"] - df["spend"]
        df["first_pay_sr"] = df["purch_count"] / df["ttp_cnt"].replace(0, float("nan"))
        df["purch_otp"] = df["ttp_cnt"] / df["pw_cnt"].replace(0, float("nan"))
        df["purch_land"] = df["purch_count"] / df["lv_cnt"].replace(0, float("nan"))
        df["upsell_rate"] = df["upsell_cnt"] / df["purch_count"].replace(0, float("nan"))
        df["chase_rate"] = df["chase_cnt"] / df["sub_cnt"].replace(0, float("nan"))
        df["em_ratio"] = df["em_sub"] / df["sub_cnt"].replace(0, float("nan"))
        df["age18_24_ratio"] = df["age18_24_purch"] / df["purch_count"].replace(0, float("nan"))
        df["week_ratio"] = df["week1_purch"] / df["purch_count"].replace(0, float("nan"))
        df["ww_ratio"] = df["ww_purch"] / df["purch_count"].replace(0, float("nan"))
        df["cpm"] = df["spend"] / df["impressions"].replace(0, float("nan")) * 1000
        df["ctr"] = df["click"] / df["impressions"].replace(0, float("nan"))
        return df

    async def get_roi(self, source: str | None = None) -> list[dict]:
        df = await self._load_roi()
        if source:
            df = df[df["utm_source"] == source]
        return df.sort_values(["date", "utm_source"]).fillna(0).to_dict("records")

    async def get_sources(self) -> list[str]:
        df = await self._load_roi()
        return sorted(df["utm_source"].dropna().unique().tolist())
