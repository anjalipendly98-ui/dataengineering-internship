"""
Clickstream Spark Job
Processes clickstream data, handles errors, and writes partitioned Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date


def main(input_path, output_path, error_path):

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Clickstream Processing") \
        .getOrCreate()

    print("Reading input data...")

    # Read CSV
    df = spark.read.option("header", True).csv(input_path)

    print("Initial count:", df.count())

    # Convert timestamp
    df = df.withColumn(
        "click_time",
        to_timestamp(col("click_time"))
    )

    # ===============================
    # ERROR HANDLING
    # ===============================

    # Invalid records (missing or bad timestamp)
    invalid_df = df.filter(col("click_time").isNull())

    print("Invalid records:", invalid_df.count())

    # Write invalid records
    invalid_df.write \
        .mode("overwrite") \
        .parquet(error_path)

    # Valid records
    valid_df = df.filter(col("click_time").isNotNull())

    print("Valid records:", valid_df.count())

    # ===============================
    # TRANSFORMATIONS
    # ===============================

    # Remove duplicates (optional)
    valid_df = valid_df.dropDuplicates()

    # Add partition column
    valid_df = valid_df.withColumn(
        "click_date",
        to_date(col("click_time"))
    )

    # ===============================
    # WRITE OUTPUT
    # ===============================

    print("Writing output...")

    valid_df.write \
        .mode("overwrite") \
        .partitionBy("click_date") \
        .parquet(output_path)

    print("Job completed successfully")

    spark.stop()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--error", required=True)

    args = parser.parse_args()

    main(args.input, args.output, args.error)