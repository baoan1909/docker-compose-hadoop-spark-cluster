import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round, sum


PROCESSED_INPUT = "hdfs://namenode:9000/output/titanic/processed"
STATS_OUTPUT = "hdfs://namenode:9000/output/titanic/stats"


def read_csv(spark, path):
    # Doc processed dataset da co them cac cot FamilySize, AgeGroup, FareGroup...
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def survival_stats(df, group_columns):
    # Ham dung chung de tinh thong ke song sot theo tung nhom phan tich.
    return (
        df.groupBy(*group_columns)
        .agg(
            count("*").alias("total_passengers"),
            # Survived la 0/1, tong Survived chinh la so hanh khach song sot.
            sum(col("Survived")).alias("survived_count"),
            # Trung binh Survived * 100 la ti le song sot theo phan tram.
            round(avg(col("Survived")) * 100, 2).alias("survival_rate_percent"),
            round(avg(col("Age")), 2).alias("avg_age"),
            round(avg(col("Fare")), 2).alias("avg_fare"),
        )
        .orderBy(*group_columns)
    )


def write_csv(df, output_path):
    # Ghi moi bang thong ke thanh mot thu muc rieng tren HDFS.
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)


def main():
    # Mac dinh doc /output/titanic/processed va ghi cac bang vao /output/titanic/stats.
    input_path = sys.argv[1] if len(sys.argv) > 1 else PROCESSED_INPUT
    output_base = sys.argv[2] if len(sys.argv) > 2 else STATS_OUTPUT

    spark = SparkSession.builder.appName("Nhom1_Titanic_Stats").getOrCreate()

    df = read_csv(spark, input_path)

    # Tao nhieu bang thong ke de phuc vu bao cao va truc quan hoa.
    outputs = {
        "survival_by_sex": survival_stats(df, ["Sex"]),
        "survival_by_pclass": survival_stats(df, ["Pclass"]),
        "survival_by_embarked": survival_stats(df, ["Embarked"]),
        "survival_by_age_group": survival_stats(df, ["AgeGroup"]),
        "survival_by_family_size": survival_stats(df, ["FamilySize"]),
        "survival_by_title": survival_stats(df, ["Title"]),
        "fare_by_pclass": df.groupBy("Pclass")
        .agg(
            count("*").alias("total_passengers"),
            round(avg("Fare"), 2).alias("avg_fare"),
            round(avg("Age"), 2).alias("avg_age"),
        )
        .orderBy("Pclass"),
    }

    # Ghi tung bang thong ke ra HDFS va in ra console de de demo.
    print("=== TitanicStats ===")
    print(f"Input rows: {df.count()}")
    for name, stat_df in outputs.items():
        print(f"--- {name} ---")
        stat_df.show(30, truncate=False)
        write_csv(stat_df, f"{output_base}/{name}")

    spark.stop()


if __name__ == "__main__":
    main()
