import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, coalesce, concat_ws, explode, length, lit, lower, regexp_replace, split


DEFAULT_INPUT = "hdfs://namenode:9000/input/test.csv"
DEFAULT_OUTPUT = "hdfs://namenode:9000/output/word_mean"


def extract_words(spark, input_path):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(input_path)
    )

    line_df = df.select(
        concat_ws(" ", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]).alias("line")
    )

    return (
        line_df.select(
            explode(split(lower(regexp_replace(col("line"), "[^A-Za-z]+", " ")), "\\s+")).alias("word")
        )
        .where(length(col("word")) > 0)
    )


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT

    spark = SparkSession.builder.appName("Nhom1_WordMean_test_csv").getOrCreate()

    words = extract_words(spark, input_path)
    result = words.select(length(col("word")).alias("word_length")).agg(
        avg("word_length").alias("word_mean")
    )

    result.write.mode("overwrite").option("header", "true").csv(output_path)
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
