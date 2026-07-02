import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, substring, trim, when


CLEANED_INPUT = "hdfs://namenode:9000/output/titanic/cleaned"
PROCESSED_OUTPUT = "hdfs://namenode:9000/output/titanic/processed"


def read_csv(spark, path):
    # Doc cleaned CSV va de Spark tu nhan kieu du lieu cho cac cot da duoc clean.
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def main():
    # Mac dinh doc /output/titanic/cleaned va ghi /output/titanic/processed tren HDFS.
    input_path = sys.argv[1] if len(sys.argv) > 1 else CLEANED_INPUT
    output_path = sys.argv[2] if len(sys.argv) > 2 else PROCESSED_OUTPUT

    spark = SparkSession.builder.appName("Nhom1_Titanic_Process").getOrCreate()

    df = read_csv(spark, input_path)

    # Feature engineering: tao cac cot moi de phan tich sau nay.
    processed_df = (
        # Tong so nguoi trong gia dinh di cung tren tau.
        df.withColumn("FamilySize", col("SibSp") + col("Parch") + 1)
        # IsAlone = 1 neu hanh khach di mot minh, nguoc lai = 0.
        .withColumn("IsAlone", when(col("FamilySize") == 1, 1).otherwise(0))
        # Title duoc tach tu Name, vi du: Mr, Mrs, Miss, Master.
        .withColumn("Title", trim(regexp_extract(col("Name"), r",\s*([^\.]+)\.", 1)))
        # Gom tuoi thanh nhom de thong ke truc quan hon.
        .withColumn(
            "AgeGroup",
            when(col("Age") < 13, "Child")
            .when((col("Age") >= 13) & (col("Age") < 18), "Teen")
            .when((col("Age") >= 18) & (col("Age") < 60), "Adult")
            .otherwise("Senior"),
        )
        # Gom gia ve thanh nhom Low/Medium/High de de phan tich.
        .withColumn(
            "FareGroup",
            when(col("Fare") < 10, "Low")
            .when((col("Fare") >= 10) & (col("Fare") < 50), "Medium")
            .otherwise("High"),
        )
        # Deck lay ky tu dau cua Cabin; neu khong co Cabin thi ghi Unknown.
        .withColumn(
            "Deck",
            when(col("Cabin").isNull() | (col("Cabin") == "Unknown"), "Unknown").otherwise(
                substring(col("Cabin"), 1, 1)
            ),
        )
        .orderBy("PassengerId")
    )

    # In mau du lieu sau xu ly de kiem tra cac cot moi da tao dung hay chua.
    print("=== TitanicProcess ===")
    print(f"Input rows: {df.count()}")
    print(f"Processed rows: {processed_df.count()}")
    processed_df.printSchema()
    processed_df.select(
        "PassengerId",
        "Survived",
        "Pclass",
        "Sex",
        "Age",
        "FamilySize",
        "IsAlone",
        "Title",
        "AgeGroup",
        "FareGroup",
        "Deck",
    ).show(20, truncate=False)

    # Ghi processed dataset ra HDFS cho buoc thong ke.
    processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
