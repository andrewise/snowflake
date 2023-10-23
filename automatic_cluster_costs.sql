-- This query provides a list of tables with Automatic Clustering and the
-- volume of credits consumed via the service over the last 30 days, broken
-- out by day. Any irregularities in the credit consumption or consistently
-- high consumption are flags for additional investigation.
SELECT TO_DATE(start_time) AS date,
  database_name,
  schema_name,
  table_name,
  SUM(credits_used) AS credits_used
FROM snowflake.account_usage.automatic_clustering_history
WHERE start_time >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2,3,4
ORDER BY 5 DESC;

-- This query shows the average daily credits consumed by Automatic Clustering
-- grouped by week over the last year. It can help identify anomalies in daily
-- averages over the year so you can investigate spikes or unexpected changes
-- in consumption.
WITH credits_by_day AS (
  SELECT TO_DATE(start_time) AS date,
    SUM(credits_used) AS credits_used
  FROM snowflake.account_usage.automatic_clustering_history
  WHERE start_time >= DATEADD(year,-1,CURRENT_TIMESTAMP())
  GROUP BY 1
  ORDER BY 2 DESC
)

SELECT DATE_TRUNC('week',date),
      AVG(credits_used) AS avg_daily_credits
FROM credits_by_day
GROUP BY 1
ORDER BY 1;