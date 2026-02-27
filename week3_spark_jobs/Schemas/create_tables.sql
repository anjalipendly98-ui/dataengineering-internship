-- Main clickstream table

CREATE TABLE `anjali-pendly-week-1.raw_spark_outputs.clickstream` (
  click_time TIMESTAMP,
  location STRING
)
PARTITION BY DATE(click_time);