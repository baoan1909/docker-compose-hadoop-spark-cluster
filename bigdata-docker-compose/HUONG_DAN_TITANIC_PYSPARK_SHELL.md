# Huong dan thuc hien bai mo rong Titanic bang PySpark shell

Tai lieu nay huong dan chay quy trinh Titanic truc tiep trong `pyspark` shell, khong dung `spark-submit` de submit file `.py`.

Luu y: Neu cum Hadoop/Spark dang chay bang Docker, ta van can dung `docker exec` de vao container `spark-master`. Sau khi da vao container va mo `pyspark`, cac lenh xu ly du lieu ben duoi deu la lenh PySpark shell.

## 1. Mo PySpark shell

Tren may master, vao container Spark master:

```bash
docker exec -it spark-master bash
```

Mo PySpark shell:

```bash
pyspark --master spark://spark-master:7077
```

Neu muon chi ro HDFS default filesystem:

```bash
pyspark --master spark://spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000
```

Khi thay dau nhac:

```text
>>>
```

thi bat dau go cac lenh PySpark ben duoi.

## 2. Dua 3 file goc len HDFS

Phan nay chay trong terminal cua container, truoc khi mo `pyspark`, hoac mo terminal khac:

```bash
hdfs dfs -mkdir -p /input/titanic
hdfs dfs -put -f /data/train.csv /input/titanic/train.csv
hdfs dfs -put -f /data/test.csv /input/titanic/test.csv
hdfs dfs -put -f /data/gender_submission.csv /input/titanic/gender_submission.csv
hdfs dfs -ls /input/titanic
```

## 3. Doc du lieu trong PySpark shell

Tu day tro di, go trong `pyspark` shell:

```python
train_df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", '"').csv("hdfs://namenode:9000/input/titanic/train.csv")
test_df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", '"').csv("hdfs://namenode:9000/input/titanic/test.csv")
gender_df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("escape", '"').csv("hdfs://namenode:9000/input/titanic/gender_submission.csv")
```

Kiem tra:

```python
train_df.printSchema()
test_df.printSchema()
gender_df.printSchema()
print("Train:", train_df.count())
print("Test:", test_df.count())
print("Gender:", gender_df.count())
```

## 4. Merge test.csv voi gender_submission.csv

`test.csv` khong co cot `Survived`, nen join voi `gender_submission.csv` theo `PassengerId`.

```python
merged_test_df = test_df.join(gender_df, on="PassengerId", how="left")
merged_test_df = merged_test_df.select(train_df.columns)
```

Kiem tra:

```python
merged_test_df.printSchema()
merged_test_df.show(10, truncate=False)
```

## 5. Gop train.csv va test da co Survived

```python
merged_df = train_df.unionByName(merged_test_df)
```

Kiem tra:

```python
print("Merged rows:", merged_df.count())
merged_df.printSchema()
merged_df.show(10, truncate=False)
```

Ghi tap merged ra HDFS:

```python
merged_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/merged")
```

## 6. Lam sach du lieu

Import cac ham can dung:

```python
from pyspark.sql.functions import col, lower, trim, upper, when
from pyspark.sql.types import IntegerType, DoubleType
```

Chuan hoa cac gia tri missing:

```python
NULL_TOKENS = ["", " ", "nan", "NaN", "null", "NULL", "None", "none"]
df = merged_df

for column_name in df.columns:
    df = df.withColumn(
        column_name,
        when(trim(col(column_name).cast("string")).isin(NULL_TOKENS), None).otherwise(col(column_name))
    )
```

Ep kieu du lieu:

```python
df = (
    df.withColumn("PassengerId", col("PassengerId").cast(IntegerType()))
      .withColumn("Survived", col("Survived").cast(IntegerType()))
      .withColumn("Pclass", col("Pclass").cast(IntegerType()))
      .withColumn("Age", col("Age").cast(DoubleType()))
      .withColumn("SibSp", col("SibSp").cast(IntegerType()))
      .withColumn("Parch", col("Parch").cast(IntegerType()))
      .withColumn("Fare", col("Fare").cast(DoubleType()))
)
```

Tinh median cho `Age`, `Fare`:

```python
age_median = df.where(col("Age").isNotNull()).approxQuantile("Age", [0.5], 0.01)[0]
fare_median = df.where(col("Fare").isNotNull()).approxQuantile("Fare", [0.5], 0.01)[0]
print("Age median:", age_median)
print("Fare median:", fare_median)
```

Tinh mode cho `Cabin`, `Embarked`:

```python
cabin_mode_row = df.where(col("Cabin").isNotNull()).groupBy("Cabin").count().orderBy(col("count").desc(), col("Cabin").asc()).first()
embarked_mode_row = df.where(col("Embarked").isNotNull()).groupBy("Embarked").count().orderBy(col("count").desc(), col("Embarked").asc()).first()

cabin_mode = cabin_mode_row["Cabin"] if cabin_mode_row else "Unknown"
embarked_mode = embarked_mode_row["Embarked"] if embarked_mode_row else "Unknown"

print("Cabin mode:", cabin_mode)
print("Embarked mode:", embarked_mode)
```

Dien gia tri thieu va chuan hoa text:

```python
cleaned_df = df.fillna({
    "Age": age_median,
    "Fare": fare_median,
    "Cabin": cabin_mode,
    "Embarked": embarked_mode
})

cleaned_df = (
    cleaned_df.withColumn("Sex", lower(trim(col("Sex"))))
              .withColumn("Embarked", upper(trim(col("Embarked"))))
              .dropDuplicates(["PassengerId"])
              .orderBy("PassengerId")
)
```

Kiem tra:

```python
print("Cleaned rows:", cleaned_df.count())
cleaned_df.printSchema()
cleaned_df.show(10, truncate=False)
```

Ghi cleaned ra HDFS:

```python
cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/cleaned")
```

## 7. Xu ly va tao dac trung moi

Import them:

```python
from pyspark.sql.functions import regexp_extract, substring
```

Tao cac cot moi:

```python
processed_df = (
    cleaned_df.withColumn("FamilySize", col("SibSp") + col("Parch") + 1)
              .withColumn("IsAlone", when(col("FamilySize") == 1, 1).otherwise(0))
              .withColumn("Title", trim(regexp_extract(col("Name"), r",\s*([^\.]+)\.", 1)))
              .withColumn(
                  "AgeGroup",
                  when(col("Age") < 13, "Child")
                  .when((col("Age") >= 13) & (col("Age") < 18), "Teen")
                  .when((col("Age") >= 18) & (col("Age") < 60), "Adult")
                  .otherwise("Senior")
              )
              .withColumn(
                  "FareGroup",
                  when(col("Fare") < 10, "Low")
                  .when((col("Fare") >= 10) & (col("Fare") < 50), "Medium")
                  .otherwise("High")
              )
              .withColumn(
                  "Deck",
                  when(col("Cabin").isNull() | (col("Cabin") == "Unknown"), "Unknown")
                  .otherwise(substring(col("Cabin"), 1, 1))
              )
              .orderBy("PassengerId")
)
```

Kiem tra:

```python
processed_df.select("PassengerId", "Survived", "Pclass", "Sex", "Age", "FamilySize", "IsAlone", "Title", "AgeGroup", "FareGroup", "Deck").show(20, truncate=False)
```

Ghi processed ra HDFS:

```python
processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/processed")
```

## 8. Thong ke du lieu

Import:

```python
from pyspark.sql.functions import avg, count, round, sum
```

Tao ham thong ke song sot:

```python
def survival_stats(df, group_columns):
    return (
        df.groupBy(*group_columns)
          .agg(
              count("*").alias("total_passengers"),
              sum(col("Survived")).alias("survived_count"),
              round(avg(col("Survived")) * 100, 2).alias("survival_rate_percent"),
              round(avg(col("Age")), 2).alias("avg_age"),
              round(avg(col("Fare")), 2).alias("avg_fare")
          )
          .orderBy(*group_columns)
    )
```

Tao cac bang thong ke:

```python
survival_by_sex = survival_stats(processed_df, ["Sex"])
survival_by_pclass = survival_stats(processed_df, ["Pclass"])
survival_by_embarked = survival_stats(processed_df, ["Embarked"])
survival_by_age_group = survival_stats(processed_df, ["AgeGroup"])
survival_by_family_size = survival_stats(processed_df, ["FamilySize"])
survival_by_title = survival_stats(processed_df, ["Title"])

fare_by_pclass = (
    processed_df.groupBy("Pclass")
                .agg(
                    count("*").alias("total_passengers"),
                    round(avg("Fare"), 2).alias("avg_fare"),
                    round(avg("Age"), 2).alias("avg_age")
                )
                .orderBy("Pclass")
)
```

Xem ket qua:

```python
survival_by_sex.show(truncate=False)
survival_by_pclass.show(truncate=False)
survival_by_embarked.show(truncate=False)
survival_by_age_group.show(truncate=False)
survival_by_family_size.show(truncate=False)
fare_by_pclass.show(truncate=False)
```

Ghi cac bang thong ke ra HDFS:

```python
survival_by_sex.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_sex")
survival_by_pclass.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_pclass")
survival_by_embarked.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_embarked")
survival_by_age_group.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_age_group")
survival_by_family_size.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_family_size")
survival_by_title.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/survival_by_title")
fare_by_pclass.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output/titanic/stats/fare_by_pclass")
```

## 9. Truc quan hoa trong PySpark shell

Luu y: buoc nay can container co `matplotlib`. Neu thieu, bao An them `matplotlib` vao Dockerfile.

Import:

```python
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
```

Tao thu muc output:

```python
output_dir = "/data/visualizations"
os.makedirs(output_dir, exist_ok=True)
```

Tao ham ve bieu do cot:

```python
def save_bar_chart(stat_df, x_column, y_column, title, xlabel, ylabel, filename):
    rows = stat_df.select(x_column, y_column).collect()
    labels = [str(row[x_column]) for row in rows]
    values = [float(row[y_column]) if row[y_column] is not None else 0.0 for row in rows]

    plt.figure(figsize=(9, 5))
    bars = plt.bar(labels, values, color="#4C78A8")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=25, ha="right")

    for bar, value in zip(bars, values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.2f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename), dpi=150)
    plt.close()
```

Ve cac bieu do:

```python
save_bar_chart(survival_by_sex, "Sex", "survival_rate_percent", "Survival Rate by Sex", "Sex", "Survival rate (%)", "survival_by_sex.png")
save_bar_chart(survival_by_pclass, "Pclass", "survival_rate_percent", "Survival Rate by Passenger Class", "Passenger class", "Survival rate (%)", "survival_by_pclass.png")
save_bar_chart(survival_by_embarked, "Embarked", "survival_rate_percent", "Survival Rate by Embarked Port", "Embarked", "Survival rate (%)", "survival_by_embarked.png")
save_bar_chart(survival_by_age_group, "AgeGroup", "survival_rate_percent", "Survival Rate by Age Group", "Age group", "Survival rate (%)", "survival_by_age_group.png")
save_bar_chart(survival_by_family_size, "FamilySize", "total_passengers", "Passenger Count by Family Size", "Family size", "Passengers", "family_size_distribution.png")
save_bar_chart(fare_by_pclass, "Pclass", "avg_fare", "Average Fare by Passenger Class", "Passenger class", "Average fare", "fare_by_pclass.png")
```

Kiem tra trong container:

```python
os.listdir(output_dir)
```

## 10. Kiem tra ket qua HDFS

Thoat PySpark shell bang:

```python
exit()
```

Kiem tra cac output:

```bash
hdfs dfs -ls /output/titanic
hdfs dfs -ls /output/titanic/stats
hdfs dfs -cat /output/titanic/stats/survival_by_sex/part-*
ls /data/visualizations
```

## 11. Noi dung mo ta trong bao cao

Co the viet:

```text
Nhom thuc hien phan mo rong bang PySpark shell tren Hadoop Cluster. Dau tien, cac tap train.csv, test.csv va gender_submission.csv duoc dua len HDFS. Trong PySpark shell, nhom doc cac tap du lieu bang Spark DataFrame, ghep test.csv voi gender_submission.csv theo PassengerId de bo sung cot Survived, sau do hop nhat voi train.csv tao thanh tap merged_titanic. Tiep theo, nhom lam sach du lieu, xu ly missing values, chuan hoa kieu du lieu, tao cac dac trung moi va tinh cac bang thong ke. Cuoi cung, cac bang thong ke duoc truc quan hoa thanh bieu do PNG phuc vu bao cao.
```

