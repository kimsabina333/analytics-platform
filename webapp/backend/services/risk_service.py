import asyncio
import time
from typing import Optional
import pandas as pd

from backend.services.db_service import DBService

CACHE_TTL = 3600  # 1 hour

# CB count: type='chargeback' AND status NOT IN ('resolved','won','cancelled')
# VAMP: fraud+CB for visa/applepay only, divided by visa settled
_RISK_SQL = """
WITH firsts AS (
    SELECT customer_account_id, payment_method, card_brand
    FROM `payments.all_payments_prod`
    WHERE channel = 'solidgate'
        AND payment_type = 'first'
        AND payment_method NOT IN ('paypal', 'paypal-vault')
        AND status = 'settled'
),
all_trans AS (
    SELECT DISTINCT
        p.order_id, p.status, p.amount, p.currency,
        p.customer_account_id, p.created_at, p.settle_interval,
        CASE
            WHEN p.mid = 'de185c91-6045-4190-babc-42558400cb92' THEN 'esquire'
            WHEN p.mid = 'd4d7b345-bf19-453a-acdc-8ea68a5d4c44' THEN 'adyen US'
        END AS mid,
        p.channel, p.payment_type, p2.payment_method,
        LOWER(p.card_brand) AS card_brand
    FROM `payments.all_payments_prod` p
    LEFT JOIN firsts p2 ON p.customer_account_id = p2.customer_account_id
    WHERE p.channel = 'solidgate' AND p.status = 'settled' AND p.mid != 'paypal'
        AND DATE(TIMESTAMP_MICROS(p.created_at)) >= '2025-01-01'
),
all_tx AS (
    SELECT
        p.order_id, FORMAT_DATE('%Y-%m', tx_date) AS month,
        p.card_brand, p.mid, p.channel, p.payment_type, p.currency,
        p.amount, ex.exchange_rate,
        (p.amount * ex.exchange_rate) / 100 AS usd_amount,
        tx_date, payment_method, p.customer_account_id
    FROM all_trans p
    LEFT JOIN `analytics_draft.exchange_rate` ex
        ON ex.date = DATE(TIMESTAMP_MICROS(p.created_at)) AND ex.currency = p.currency
    CROSS JOIN UNNEST([DATE(
        CASE WHEN p.settle_interval IS NULL OR p.settle_interval = 0
            THEN TIMESTAMP_MICROS(p.created_at)
            ELSE TIMESTAMP_ADD(TIMESTAMP_MICROS(p.created_at), INTERVAL p.settle_interval HOUR)
        END)]) AS tx_date
    UNION ALL
    SELECT
        p.order_id, FORMAT_DATE('%Y-%m', tx_date) AS month,
        CASE
            WHEN LOWER(p.card_brand) IN ('mastercard', 'mc') THEN 'mastercard'
            WHEN LOWER(p.card_brand) = 'applepay' THEN 'visa'
            ELSE LOWER(p.card_brand)
        END AS card_brand,
        p.mid, p.channel, p.payment_type, p.currency, p.amount, ex.exchange_rate,
        (p.amount * ex.exchange_rate) / 100 AS usd_amount,
        tx_date, payment_method, p.customer_account_id
    FROM `payments.all_payments_prod` p
    LEFT JOIN `analytics_draft.exchange_rate` ex
        ON ex.date = DATE(TIMESTAMP_MICROS(p.created_at)) AND ex.currency = p.currency
    CROSS JOIN UNNEST([DATE(
        CASE WHEN p.settle_interval IS NULL OR p.settle_interval = 0
            THEN TIMESTAMP_MICROS(p.created_at)
            ELSE TIMESTAMP_ADD(TIMESTAMP_MICROS(p.created_at), INTERVAL p.settle_interval HOUR)
        END)]) AS tx_date
    WHERE p.status = 'settled' AND p.channel != 'solidgate'
),
combined AS (
    SELECT
        order_id, tx_date AS date_of_transaction, usd_amount AS amount,
        customer_account_id, payment_method, card_brand, channel,
        CASE
            WHEN mid = 'adyen US'                              THEN 'adyen us (solidgate)'
            WHEN mid = 'de185c91-6045-4190-babc-42558400cb92' THEN 'esquire'
            WHEN mid = 'adyen_us'                              THEN 'adyen us (primer)'
            WHEN mid = 'adyen'                                 THEN 'adyen uae'
            ELSE mid
        END AS mid,
        '' AS reason, '' AS status, 'settled' AS type
    FROM all_tx
    UNION ALL
    SELECT
        order_id, fraud_issue_date, fraud_amount_usd,
        customer_account_id,
        CASE WHEN payment_method = 'recurring' THEN payment_method_first ELSE payment_method END,
        card_brand, channel, mid, fraud_reason, '' AS status, 'fraud' AS type
    FROM `analytics_draft.fraud_final`
    UNION ALL
    SELECT
        order_id, dispute_issue_date, dispute_amount_usd,
        customer_account_id,
        CASE WHEN payment_method = 'recurring' THEN payment_method_first ELSE payment_method END,
        card_brand, channel, mid, reason_code_processed, status_processed, 'chargeback' AS type
    FROM `analytics_draft.chargebacks_final`
)
SELECT
    FORMAT_DATE('%Y-%m', c.date_of_transaction) AS month,
    c.mid,
    COUNT(DISTINCT CASE WHEN c.type = 'settled' THEN c.order_id END)                              AS settled_count,
    SUM(CASE WHEN c.type = 'settled' THEN c.amount ELSE 0 END)                                    AS settled_usd,
    COUNT(DISTINCT CASE
        WHEN c.type = 'chargeback'
             AND c.status NOT IN ('resolved', 'won', 'cancelled')
        THEN c.order_id END)                                                                        AS cb_count,
    SUM(CASE
        WHEN c.type = 'chargeback'
             AND c.status NOT IN ('resolved', 'won', 'cancelled')
        THEN c.amount ELSE 0 END)                                                                   AS cb_usd,
    COUNT(DISTINCT CASE WHEN c.type = 'fraud' THEN c.order_id END)                                AS fraud_count,
    SUM(CASE WHEN c.type = 'fraud' THEN c.amount ELSE 0 END)                                      AS fraud_usd,
    COUNT(DISTINCT CASE
        WHEN c.type IN ('fraud', 'chargeback')
             AND LOWER(c.card_brand) IN ('visa', 'applepay')
             AND (c.type = 'fraud' OR c.status NOT IN ('resolved', 'won', 'cancelled'))
        THEN c.order_id END)                                                                        AS vamp_dispute_count,
    COUNT(DISTINCT CASE
        WHEN c.type = 'settled' AND LOWER(c.card_brand) IN ('visa', 'applepay')
        THEN c.order_id END)                                                                        AS visa_settled_count
FROM combined c
WHERE c.date_of_transaction >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    AND c.date_of_transaction IS NOT NULL
    AND c.mid IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
"""

_COR_SQL = """
WITH checkout AS (
    SELECT
        'checkout' AS merchant_account,
        breakdown_type,
        DATE_TRUNC(DATE(requested_on), MONTH) AS month,
        SUM(
            CASE
                WHEN holding_currency = 'AED' THEN holding_currency_amount * ex.exchange_rate
                WHEN holding_currency = 'USD' THEN holding_currency_amount
                ELSE holding_currency_amount
            END
        ) AS total_usd
    FROM `hopeful-list-429812-f3.analytics_draft.cko_financial_actions`
    LEFT JOIN `hopeful-list-429812-f3.analytics_draft.exchange_rate` ex
        ON holding_currency = ex.currency
       AND DATE(requested_on) = ex.date
    WHERE requested_on >= TIMESTAMP('2025-01-01')
    GROUP BY 1, 2, 3
),
adyen_base AS (
    SELECT
        CASE
            WHEN merchant_account = 'JobescapeCOM' THEN 'adyen uae'
            WHEN merchant_account = 'JobescapeCOM_US' THEN 'adyen us'
        END AS merchant_account,
        DATE_TRUNC(DATE(booking_date), MONTH) AS month,
        record_type,
        LOWER(COALESCE(global_card_brand, '')) AS card_brand,
        COALESCE(main_amount, 0) AS main_amount,
        COALESCE(commission_sc, 0) AS commission_sc,
        COALESCE(markup_sc, 0) AS markup_sc,
        COALESCE(scheme_fees_sc, 0) AS scheme_fees_sc,
        COALESCE(interchange_sc, 0) AS interchange_sc,
        COALESCE(processing_fee_fc, 0) AS processing_fee_fc
    FROM `hopeful-list-429812-f3.analytics_draft.adyen_payment_accounting`
    WHERE DATE(booking_date) >= DATE('2025-01-01')
      AND merchant_account IN ('JobescapeCOM', 'JobescapeCOM_US')
),
adyen_breakdown AS (
    SELECT merchant_account, 'Authorisation Scheme Fees (Refused)' AS breakdown_type, month, SUM(scheme_fees_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'Refused' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Authorisation Scheme Fees (Retried)' AS breakdown_type, month, SUM(scheme_fees_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'Retried' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Chargeback Amount' AS breakdown_type, month, SUM(main_amount) AS total_usd
    FROM adyen_base WHERE record_type = 'Chargeback' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Chargeback Reversed Amount' AS breakdown_type, month, SUM(main_amount) AS total_usd
    FROM adyen_base WHERE record_type = 'ChargebackReversed' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Blend (amex)' AS breakdown_type, month, SUM(commission_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' AND card_brand = 'amex' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Blend (discover)' AS breakdown_type, month, SUM(commission_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' AND card_brand = 'discover' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Blend Chargebacks' AS breakdown_type, month, SUM(commission_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'Chargeback' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Blend Reversed Chargebacks' AS breakdown_type, month, SUM(commission_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'ChargebackReversed' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission' AS breakdown_type, month, SUM(commission_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' AND card_brand NOT IN ('amex', 'discover') GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Markup' AS breakdown_type, month, SUM(markup_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Markup Chargebacks' AS breakdown_type, month, SUM(markup_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'Chargeback' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Markup Reversed Chargebacks' AS breakdown_type, month, SUM(markup_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'ChargebackReversed' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Commission Markup Second Chargebacks' AS breakdown_type, month, SUM(markup_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SecondChargeback' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Gross Revenue (SentForSettle)' AS breakdown_type, month, SUM(main_amount) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Interchange Issuing Banks' AS breakdown_type, month, SUM(interchange_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Processing Fee' AS breakdown_type, month, SUM(processing_fee_fc) AS total_usd
    FROM adyen_base WHERE processing_fee_fc != 0 GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Refund Amount' AS breakdown_type, month, SUM(main_amount) AS total_usd
    FROM adyen_base WHERE record_type = 'Refunded' GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Scheme fee Visa & Mastercard' AS breakdown_type, month, SUM(scheme_fees_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SentForSettle' AND card_brand IN ('visa', 'mc', 'mastercard') GROUP BY 1, 2, 3
    UNION ALL
    SELECT merchant_account, 'Scheme fee Visa & Mastercard Second Chargebacks' AS breakdown_type, month, SUM(scheme_fees_sc) AS total_usd
    FROM adyen_base WHERE record_type = 'SecondChargeback' AND card_brand IN ('visa', 'mc', 'mastercard') GROUP BY 1, 2, 3
)
SELECT merchant_account, breakdown_type, month, total_usd
FROM checkout
WHERE total_usd != 0
UNION ALL
SELECT merchant_account, breakdown_type, month, total_usd
FROM adyen_breakdown
WHERE total_usd != 0
ORDER BY month, merchant_account, breakdown_type
"""

_REVENUE_SQL = """
WITH firsts AS (
    SELECT
        customer_account_id,
        payment_method,
        card_brand
    FROM `payments.all_payments_prod`
    WHERE channel = 'solidgate'
      AND payment_type = 'first'
      AND payment_method NOT IN ('paypal', 'paypal-vault')
      AND status = 'settled'
),
payments AS (
    SELECT
        app.order_id,
        (app.amount * fx.exchange_rate) / 100 AS amount_usd,
        app.customer_account_id,
        TIMESTAMP_MICROS(app.created_at) AS created_at,
        TIMESTAMP_ADD(
            TIMESTAMP_MICROS(app.settle_datetime),
            INTERVAL IF(app.settle_interval IS NULL, 0, app.settle_interval) HOUR
        ) AS settle_datetime,
        CASE
            WHEN app.payment_method = 'recurring' THEN p2.payment_method
            ELSE app.payment_method
        END AS payment_method,
        app.payment_type,
        LOWER(app.card_brand) AS card_brand,
        CASE
            WHEN app.mid = 'adyen_us' THEN 'adyen_us (Primer)'
            WHEN app.mid = 'adyen' THEN 'adyen uae'
            ELSE app.mid
        END AS mid,
        app.bin,
        app.channel,
        IF(app.settle_interval IS NULL, 0, app.settle_interval) AS settle_interval,
        app.issuing_bank
    FROM `payments.all_payments_prod` AS app
    LEFT JOIN `analytics_draft.exchange_rate` AS fx
        ON fx.date = DATE(TIMESTAMP_MICROS(app.created_at))
       AND fx.currency = app.currency
    LEFT JOIN firsts AS p2
        ON app.customer_account_id = p2.customer_account_id
    WHERE app.status = 'settled'
      AND (app.amount * fx.exchange_rate) IS NOT NULL
),
final_table AS (
    SELECT
        p.*,
        l.status,
        l.settled_at
    FROM payments p
    LEFT JOIN `payments.payments_lifecycle` l
        ON p.order_id = l.order_id
),
events AS (
    SELECT
        order_id,
        created_at AS event_date,
        'Authorization' AS event_type,
        amount_usd,
        mid,
        payment_type,
        payment_method,
        card_brand,
        channel
    FROM final_table
    UNION ALL
    SELECT
        order_id,
        settle_datetime AS event_date,
        'Settlement' AS event_type,
        amount_usd,
        mid,
        payment_type,
        payment_method,
        card_brand,
        channel
    FROM final_table
    WHERE settle_datetime IS NOT NULL
    UNION ALL
    SELECT
        order_id,
        settled_at AS event_date,
        'Lifecycle Settled' AS event_type,
        amount_usd,
        mid,
        payment_type,
        payment_method,
        card_brand,
        channel
    FROM final_table
    WHERE settled_at IS NOT NULL
)
SELECT
    FORMAT_DATE('%Y-%m', DATE(event_date)) AS month,
    event_type,
    mid,
    SUM(amount_usd) AS revenue_usd,
    COUNT(DISTINCT order_id) AS order_count
FROM events
WHERE event_date >= TIMESTAMP('2025-01-01')
  AND event_date IS NOT NULL
  AND mid IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Alert thresholds (Visa/MC network standards)
THRESHOLDS = {
    "cb_rate":    {"warn": 0.006, "alert": 0.009},   # Visa EAP: >0.9%
    "fraud_rate": {"warn": 0.015, "alert": 0.020},   # >2% triggers review
    "vamp_rate":  {"warn": 0.006, "alert": 0.009},   # Visa VAMP: >0.9%
}


def _compute_rates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cb_rate"]    = df["cb_count"]          / df["settled_count"].replace(0, float("nan"))
    df["fraud_rate"] = df["fraud_count"]        / df["settled_count"].replace(0, float("nan"))
    df["vamp_rate"]  = df["vamp_dispute_count"] / df["visa_settled_count"].replace(0, float("nan"))
    for col, thr in THRESHOLDS.items():
        df[f"{col}_status"] = "ok"
        df.loc[df[col] > thr["warn"],  f"{col}_status"] = "warn"
        df.loc[df[col] > thr["alert"], f"{col}_status"] = "alert"
    return df


class RiskService:
    def __init__(self, bq_client, db_svc: Optional[DBService] = None):
        self.bq_client = bq_client
        self.db = db_svc
        self._cache: pd.DataFrame | None = None
        self._cache_time: float = 0
        self._cor_cache: pd.DataFrame | None = None
        self._cor_cache_time: float = 0
        self._revenue_cache: pd.DataFrame | None = None
        self._revenue_cache_time: float = 0
        self._lock = asyncio.Lock()
        self._cor_lock = asyncio.Lock()
        self._revenue_lock = asyncio.Lock()

    def _df_from_persistent_cache(self, key: str) -> pd.DataFrame | None:
        if self.db is None:
            return None
        cached = self.db.cache_get(key)
        if not cached:
            return None
        return pd.read_json(cached, orient="records")

    def _df_to_persistent_cache(self, key: str, df: pd.DataFrame) -> None:
        if self.db is None:
            return
        self.db.cache_set(key, df.to_json(orient="records", date_format="iso"))

    async def _load(self) -> pd.DataFrame:
        async with self._lock:
            if self._cache is not None and time.time() - self._cache_time < CACHE_TTL:
                return self._cache
            cached = self._df_from_persistent_cache("risk:metrics:v1")
            if cached is not None:
                self._cache = _compute_rates(cached)
                self._cache_time = time.time()
                return self._cache
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: pd.DataFrame([dict(r) for r in self.bq_client.query(_RISK_SQL).result()])
            )
            self._df_to_persistent_cache("risk:metrics:v1", raw)
            self._cache = _compute_rates(raw)
            self._cache_time = time.time()
            return self._cache

    async def get_mids(self) -> list[str]:
        df = await self._load()
        return sorted(df["mid"].dropna().unique().tolist())

    async def get_trends(self, mid: str | None = None) -> list[dict]:
        df = await self._load()
        if mid:
            df = df[df["mid"] == mid]
        return df.sort_values("month").fillna(0).to_dict("records")

    async def get_summary(self) -> list[dict]:
        """Latest month per MID with alert flags — used for dashboard cards."""
        df = await self._load()
        latest = (
            df.sort_values("month")
            .groupby("mid", as_index=False)
            .last()
        )
        return latest.fillna(0).to_dict("records")

    async def get_anomalies(self) -> list[dict]:
        """Return MIDs where any metric is in alert/warn state this month."""
        summary = await self.get_summary()
        flags = []
        for row in summary:
            issues = []
            for metric, thr in THRESHOLDS.items():
                val = row.get(metric, 0) or 0
                if val > thr["alert"]:
                    issues.append({"metric": metric, "value": val, "level": "alert"})
                elif val > thr["warn"]:
                    issues.append({"metric": metric, "value": val, "level": "warn"})
            if issues:
                flags.append({"mid": row["mid"], "month": row.get("month", ""), "issues": issues})
        return flags

    async def _load_cor(self) -> pd.DataFrame:
        async with self._cor_lock:
            if self._cor_cache is not None and time.time() - self._cor_cache_time < CACHE_TTL:
                return self._cor_cache
            cached = self._df_from_persistent_cache("risk:cor:v1")
            if cached is not None:
                cached["month"] = pd.to_datetime(cached["month"]).dt.strftime("%Y-%m")
                self._cor_cache = cached
                self._cor_cache_time = time.time()
                return self._cor_cache
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: pd.DataFrame([dict(r) for r in self.bq_client.query(_COR_SQL).result()])
            )
            raw["month"] = pd.to_datetime(raw["month"]).dt.strftime("%Y-%m")
            self._df_to_persistent_cache("risk:cor:v1", raw)
            self._cor_cache = raw
            self._cor_cache_time = time.time()
            return self._cor_cache

    async def get_cor_breakdown(
        self,
        breakdown_type: str | None = None,
        merchant_account: str | None = None,
    ) -> list[dict]:
        """Cost of Revenue monthly USD totals from CKO financial actions."""
        df = await self._load_cor()
        if merchant_account:
            df = df[df["merchant_account"] == merchant_account]
        if breakdown_type:
            df = df[df["breakdown_type"] == breakdown_type]
        return df.sort_values(["month", "merchant_account", "breakdown_type"]).fillna(0).to_dict("records")

    async def get_cor_summary(self) -> list[dict]:
        """Latest month Cost of Revenue by breakdown type."""
        df = await self._load_cor()
        latest_month = df["month"].max()
        latest = df[df["month"] == latest_month].sort_values("total_usd", ascending=False)
        return latest.fillna(0).to_dict("records")

    async def _load_revenue(self) -> pd.DataFrame:
        async with self._revenue_lock:
            if self._revenue_cache is not None and time.time() - self._revenue_cache_time < CACHE_TTL:
                return self._revenue_cache
            cached = self._df_from_persistent_cache("risk:revenue:v1")
            if cached is not None:
                cached["month"] = pd.to_datetime(cached["month"]).dt.strftime("%Y-%m")
                self._revenue_cache = cached
                self._revenue_cache_time = time.time()
                return self._revenue_cache
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(
                None, lambda: pd.DataFrame([dict(r) for r in self.bq_client.query(_REVENUE_SQL).result()])
            )
            raw["month"] = pd.to_datetime(raw["month"]).dt.strftime("%Y-%m")
            self._df_to_persistent_cache("risk:revenue:v1", raw)
            self._revenue_cache = raw
            self._revenue_cache_time = time.time()
            return self._revenue_cache

    async def get_revenue(
        self,
        mid: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        """Monthly settled revenue by MID and event date type."""
        df = await self._load_revenue()
        if mid:
            df = df[df["mid"] == mid]
        if event_type:
            df = df[df["event_type"] == event_type]
        return df.sort_values(["month", "event_type", "mid"]).fillna(0).to_dict("records")

    async def get_cor_revenue_ratio(
        self,
        mid: str | None = None,
        event_type: str = "Settlement",
    ) -> list[dict]:
        """Monthly CoR as a share of revenue by MID."""
        cor = (await self._load_cor()).copy()
        revenue = (await self._load_revenue()).copy()

        non_cost_breakdowns = {"Gross Revenue (SentForSettle)", "Capture"}
        cor = cor[~cor["breakdown_type"].isin(non_cost_breakdowns)]
        cor["cor_usd"] = cor["total_usd"].abs()

        revenue["mid"] = revenue["mid"].replace({
            "adyen_us (Primer)": "adyen us",
            "adyen US": "adyen us",
            "adyen": "adyen uae",
        })
        if event_type:
            revenue = revenue[revenue["event_type"] == event_type]

        cor_monthly = (
            cor.groupby(["month", "merchant_account"], as_index=False)["cor_usd"]
            .sum()
            .rename(columns={"merchant_account": "mid"})
        )
        revenue_monthly = (
            revenue.groupby(["month", "mid"], as_index=False)
            .agg(revenue_usd=("revenue_usd", "sum"), order_count=("order_count", "sum"))
        )

        merged = cor_monthly.merge(revenue_monthly, on=["month", "mid"], how="outer")
        merged["cor_usd"] = merged["cor_usd"].fillna(0)
        merged["revenue_usd"] = merged["revenue_usd"].fillna(0)
        merged["order_count"] = merged["order_count"].fillna(0).astype(int)
        merged["cor_revenue_pct"] = (
            merged["cor_usd"] / merged["revenue_usd"].replace(0, float("nan"))
        )
        merged["event_type"] = event_type

        if mid:
            merged = merged[merged["mid"] == mid]

        return merged.sort_values(["month", "mid"]).fillna(0).to_dict("records")
