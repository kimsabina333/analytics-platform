WITH ordered_events AS (
  SELECT
    art.user_id,
    art.event_name,
    art.timestamp,
    f.subscription_cohort_date, 
    LAG(art.timestamp) OVER (PARTITION BY art.user_id ORDER BY art.timestamp) AS prev_ts
  FROM `events.app-raw-table` art
  INNER JOIN funnel f ON art.user_id = f.customer_account_id
  WHERE art.timestamp >= f.subscription_cohort_date 
    AND art.timestamp <= TIMESTAMP_ADD(f.subscription_cohort_date, INTERVAL 72 HOUR) 
),

session_flags AS (
  SELECT
    *,
    IF(prev_ts IS NULL OR TIMESTAMP_DIFF(timestamp, prev_ts, MINUTE) > 30, 1, 0) AS is_new_session
  FROM ordered_events
),

sessions AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
        PARTITION BY user_id ORDER BY timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM session_flags
),

session_metrics AS (
  SELECT
    user_id,
    session_id,
    TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), SECOND) AS session_duration_sec
  FROM sessions
  GROUP BY 1, 2
)

SELECT
    user_id AS customer_account_id,
    ROUND(SUM(session_duration_sec) / 60, 3) AS total_time_3d_min,
    --COUNT(session_id) AS total_sessions_12h
FROM session_metrics
GROUP BY 1