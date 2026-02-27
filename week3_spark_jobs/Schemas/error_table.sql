-- Error table for invalid clickstream records

CREATE TABLE `anjali-pendly-week-1.raw_spark_outputs.clickstream_errors` (
  click_time STRING,
  location STRING,
  error_reason STRING
);