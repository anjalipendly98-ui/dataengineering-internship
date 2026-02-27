-- ========================================
-- WEEK 3 ANALYTICAL QUERIES
-- ========================================

-- 1. Count events per location
SELECT
  location,
  COUNT(*) AS total_events
FROM `anjali-pendly-week-1.raw_spark_outputs.clickstream`
GROUP BY location
ORDER BY total_events DESC;


-- 2. Daily Active Users by Location
SELECT
  DATE(click_time) AS date,
  location,
  COUNT(*) AS daily_active_users
FROM `anjali-pendly-week-1.raw_spark_outputs.clickstream`
GROUP BY date, location
ORDER BY date DESC;


-- 3. Last 30 days activity
SELECT
  DATE(click_time) AS date,
  COUNT(*) AS total_events
FROM `anjali-pendly-week-1.raw_spark_outputs.clickstream`
WHERE DATE(click_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC;