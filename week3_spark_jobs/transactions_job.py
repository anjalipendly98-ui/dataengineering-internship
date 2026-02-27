import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

def main(txns_input, rates_input, output_path, error_path):
    spark = SparkSession.builder.appName("transactions_job").getOrCreate()

    txns = spark.read.option("header", True).csv(txns_input)
    rates = spark.read.option("header", True).csv(rates_input)

    txns = (
        txns
        .withColumn("txn_ts", to_timestamp(col("txn_time")))
        .withColumn("txn_date", to_date(col("txn_ts")))
        .withColumn("amount_num", col("amount").cast("double"))
    )

    rates = rates.withColumn("rate_to_usd_num", col("rate_to_usd").cast("double"))

    invalid = txns.filter(
        col("txn_id").isNull() |
        col("user_id").isNull() |
        col("txn_ts").isNull() |
        col("amount_num").isNull()
    )

    valid = txns.subtract(invalid)

    joined = valid.join(
        rates.select("currency", "rate_to_usd_num"),
        on="currency",
        how="left"
    )

    enriched = joined.withColumn(
        "amount_in_usd",
        col("amount_num") * col("rate_to_usd_num")
    )

    enriched.write.mode("append").partitionBy("txn_date").parquet(output_path)
    invalid.write.mode("append").parquet(error_path)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--txns_input", required=True)
    parser.add_argument("--rates_input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--error", required=True)
    args = parser.parse_args()

    main(args.txns_input, args.rates_input, args.output, args.error)