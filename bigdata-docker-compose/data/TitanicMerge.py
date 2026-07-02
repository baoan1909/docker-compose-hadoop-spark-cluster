import sys

from pyspark.sql import SparkSession


TRAIN_INPUT = "hdfs://namenode:9000/input/titanic/train.csv"
TEST_INPUT = "hdfs://namenode:9000/input/titanic/test.csv"
GENDER_INPUT = "hdfs://namenode:9000/input/titanic/gender_submission.csv"
MERGED_OUTPUT = "hdfs://namenode:9000/output/titanic/merged"


def read_csv(spark, path):
    # Doc CSV tu HDFS. header=true de lay ten cot, inferSchema=true de Spark tu doan kieu du lieu ban dau.
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )


def main():
    # Cho phep truyen duong dan khi chay spark-submit; neu khong truyen thi dung cac path mac dinh tren HDFS.
    train_input = sys.argv[1] if len(sys.argv) > 1 else TRAIN_INPUT
    test_input = sys.argv[2] if len(sys.argv) > 2 else TEST_INPUT
    gender_input = sys.argv[3] if len(sys.argv) > 3 else GENDER_INPUT
    output_path = sys.argv[4] if len(sys.argv) > 4 else MERGED_OUTPUT

    spark = SparkSession.builder.appName("Nhom1_Titanic_Merge").getOrCreate()

    train_df = read_csv(spark, train_input)
    test_df = read_csv(spark, test_input)
    gender_df = read_csv(spark, gender_input)

    # test.csv khong co cot Survived, nen join voi gender_submission.csv theo PassengerId de bo sung cot Survived.
    merged_test_df = test_df.join(gender_df, on="PassengerId", how="left")

    # Sap xep lai thu tu cot cua test sau khi merge cho giong train.csv, de unionByName an toan hon.
    merged_test_df = merged_test_df.select(train_df.columns)

    # Gop train.csv va test da co nhan Survived thanh tap du lieu Titanic hop nhat.
    merged_df = train_df.unionByName(merged_test_df)

    # In thong tin ra console de khi demo/chup man hinh co bang chung so dong va schema.
    print("=== TitanicMerge ===")
    print(f"Train rows: {train_df.count()}")
    print(f"Test rows: {test_df.count()}")
    print(f"Gender submission rows: {gender_df.count()}")
    print(f"Merged rows: {merged_df.count()}")
    merged_df.printSchema()
    merged_df.show(10, truncate=False)

    # coalesce(1) de output chi gom 1 file part CSV, de doc va tai ve de hon trong bai bao cao.
    merged_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
