import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, upper, when
from pyspark.sql.types import DoubleType, IntegerType


MERGED_INPUT = "hdfs://namenode:9000/output/titanic/merged"
CLEANED_OUTPUT = "hdfs://namenode:9000/output/titanic/cleaned"
NULL_TOKENS = ["", " ", "nan", "NaN", "null", "NULL", "None", "none"]


def read_csv(spark, path):
    # Doc output merged tu HDFS. inferSchema=false de minh ep kieu chu dong o buoc clean.
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )


def normalize_nulls(df):
    # Chuyen cac cach bieu dien missing value trong CSV ve null that cua Spark.
    for column_name in df.columns:
        df = df.withColumn(
            column_name,
            when(trim(col(column_name).cast("string")).isin(NULL_TOKENS), None).otherwise(
                col(column_name)
            ),
        )
    return df


def mode_value(df, column_name, fallback):
    # Lay gia tri xuat hien nhieu nhat de dien cho cot phan loai nhu Cabin hoac Embarked.
    rows = (
        df.where(col(column_name).isNotNull())
        .groupBy(column_name)
        .count()
        .orderBy(col("count").desc(), col(column_name).asc())
        .limit(1)
        .collect()
    )
    return rows[0][column_name] if rows else fallback


def median_value(df, column_name, fallback):
    # approxQuantile giup tinh median tren du lieu lon ma khong phai collect toan bo ve driver.
    values = df.where(col(column_name).isNotNull()).approxQuantile(column_name, [0.5], 0.01)
    return values[0] if values else fallback


def main():
    # Mac dinh doc /output/titanic/merged va ghi /output/titanic/cleaned tren HDFS.
    input_path = sys.argv[1] if len(sys.argv) > 1 else MERGED_INPUT
    output_path = sys.argv[2] if len(sys.argv) > 2 else CLEANED_OUTPUT

    spark = SparkSession.builder.appName("Nhom1_Titanic_Clean").getOrCreate()

    raw_df = read_csv(spark, input_path)
    df = normalize_nulls(raw_df)

    # Ep kieu cac cot so de co the tinh median, aggregate va ve thong ke chinh xac.
    df = (
        df.withColumn("PassengerId", col("PassengerId").cast(IntegerType()))
        .withColumn("Survived", col("Survived").cast(IntegerType()))
        .withColumn("Pclass", col("Pclass").cast(IntegerType()))
        .withColumn("Age", col("Age").cast(DoubleType()))
        .withColumn("SibSp", col("SibSp").cast(IntegerType()))
        .withColumn("Parch", col("Parch").cast(IntegerType()))
        .withColumn("Fare", col("Fare").cast(DoubleType()))
    )

    age_median = median_value(df, "Age", 0.0)
    fare_median = median_value(df, "Fare", 0.0)
    cabin_mode = mode_value(df, "Cabin", "Unknown")
    embarked_mode = mode_value(df, "Embarked", "Unknown")

    # Dien gia tri thieu: Age/Fare bang median, Cabin/Embarked bang mode nhu logic bai Titanic cu.
    df = df.fillna(
        {
            "Age": age_median,
            "Fare": fare_median,
            "Cabin": cabin_mode,
            "Embarked": embarked_mode,
        }
    )

    # Chuan hoa text va loai trung lap theo PassengerId de moi hanh khach chi con 1 dong.
    df = (
        df.withColumn("Sex", lower(trim(col("Sex"))))
        .withColumn("Embarked", upper(trim(col("Embarked"))))
        .dropDuplicates(["PassengerId"])
        .orderBy("PassengerId")
    )

    # In log de kiem tra nhanh qua trinh clean khi chay spark-submit.
    print("=== TitanicClean ===")
    print(f"Raw rows: {raw_df.count()}")
    print(f"Cleaned rows: {df.count()}")
    print(f"Age median: {age_median}")
    print(f"Fare median: {fare_median}")
    print(f"Cabin fill value: {cabin_mode}")
    print(f"Embarked fill value: {embarked_mode}")
    df.printSchema()
    df.show(10, truncate=False)

    # Ghi du lieu da clean ra HDFS cho cac buoc process/stats doc tiep.
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
