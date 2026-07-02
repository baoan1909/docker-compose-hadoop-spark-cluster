# Huong dan thuc hien bai mo rong Titanic bang PySpark tren Hadoop Cluster

Tai lieu nay huong dan chay lai quy trinh Titanic cu cua nhom bang PySpark tren Hadoop Cluster. Quy trinh su dung 3 file goc:

```text
train.csv
test.csv
gender_submission.csv
```

Sau do thuc hien:

```text
Merge du lieu -> Clean du lieu -> Xu ly tao dac trung -> Thong ke -> Truc quan hoa
```

## 1. Muc tieu

Muc tieu cua phan mo rong la dua du lieu Titanic len HDFS va xu ly bang PySpark tren Hadoop Cluster.

Pipeline:

```text
/input/titanic/train.csv
/input/titanic/test.csv
/input/titanic/gender_submission.csv
        |
        v
TitanicMerge.py
        |
        v
/output/titanic/merged
        |
        v
TitanicClean.py
        |
        v
/output/titanic/cleaned
        |
        v
TitanicProcess.py
        |
        v
/output/titanic/processed
        |
        v
TitanicStats.py
        |
        v
/output/titanic/stats/*
        |
        v
TitanicVisualize.py
        |
        v
/data/visualizations/*.png
```

## 2. Cac file can co

Trong thu muc:

```text
bigdata-docker-compose/data/
```

can co cac file du lieu:

```text
train.csv
test.csv
gender_submission.csv
```

va cac file chuong trinh PySpark:

```text
TitanicMerge.py
TitanicClean.py
TitanicProcess.py
TitanicStats.py
TitanicVisualize.py
```

Vai tro tung file:

```text
TitanicMerge.py
  Doc train.csv, test.csv, gender_submission.csv tu HDFS.
  Join test.csv voi gender_submission.csv theo PassengerId de bo sung cot Survived.
  Gop train.csv va test da co Survived thanh tap merged.

TitanicClean.py
  Doc tap merged.
  Chuan hoa missing values.
  Ep kieu du lieu.
  Dien Age/Fare bang median.
  Dien Cabin/Embarked bang mode.
  Xoa trung lap theo PassengerId.

TitanicProcess.py
  Doc tap cleaned.
  Tao cac cot moi: FamilySize, IsAlone, Title, AgeGroup, FareGroup, Deck.

TitanicStats.py
  Doc tap processed.
  Tinh cac bang thong ke: survival by sex, pclass, embarked, age group, family size, title.

TitanicVisualize.py
  Doc cac bang thong ke.
  Ve bieu do PNG va luu vao /data/visualizations.
```

## 3. Kiem tra cluster truoc khi chay

Tren may master, kiem tra container:

```bash
docker ps
```

Can thay cac container chinh:

```text
namenode
resourcemanager
spark-master
```

Tren slave can co:

```text
datanode1
nodemanager1
spark-worker1
```

Kiem tra HDFS va Spark:

```bash
docker exec -it namenode hdfs dfsadmin -report
docker exec -it resourcemanager yarn node -list
docker exec -it spark-master spark-submit --version
```

Neu may dung Docker Compose V1 thi lenh khoi dong compose la:

```bash
docker-compose -f docker-compose-master.yml up -d
```

Neu may dung Docker Compose V2 thi lenh la:

```bash
docker compose -f docker-compose-master.yml up -d
```

## 4. Dua du lieu len HDFS

Tren may master, chay:

```bash
docker exec -it namenode hdfs dfs -mkdir -p /input/titanic
docker exec -it namenode hdfs dfs -put -f /data/train.csv /input/titanic/train.csv
docker exec -it namenode hdfs dfs -put -f /data/test.csv /input/titanic/test.csv
docker exec -it namenode hdfs dfs -put -f /data/gender_submission.csv /input/titanic/gender_submission.csv
```

Kiem tra:

```bash
docker exec -it namenode hdfs dfs -ls /input/titanic
```

Ket qua mong muon:

```text
/input/titanic/train.csv
/input/titanic/test.csv
/input/titanic/gender_submission.csv
```

## 5. Buoc 1 - Merge du lieu

Chay:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicMerge.py
```

File nay thuc hien:

```text
test.csv + gender_submission.csv -> merged_test_df
train.csv + merged_test_df -> merged_titanic
```

Kiem tra output:

```bash
docker exec -it namenode hdfs dfs -ls /output/titanic/merged
docker exec -it namenode hdfs dfs -cat /output/titanic/merged/part-* | head
```

Output:

```text
hdfs://namenode:9000/output/titanic/merged
```

## 6. Buoc 2 - Lam sach du lieu

Chay:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicClean.py
```

File nay thuc hien:

```text
Chuan hoa null: '', 'nan', 'null', 'None' -> null
Ep kieu cac cot so
Age -> fill bang median
Fare -> fill bang median
Cabin -> fill bang mode
Embarked -> fill bang mode
Sex -> lowercase
Embarked -> uppercase
Drop duplicate theo PassengerId
```

Kiem tra output:

```bash
docker exec -it namenode hdfs dfs -ls /output/titanic/cleaned
docker exec -it namenode hdfs dfs -cat /output/titanic/cleaned/part-* | head
```

Output:

```text
hdfs://namenode:9000/output/titanic/cleaned
```

## 7. Buoc 3 - Xu ly va tao dac trung

Chay:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicProcess.py
```

File nay tao them cac cot:

```text
FamilySize = SibSp + Parch + 1
IsAlone = 1 neu FamilySize = 1, nguoc lai = 0
Title = tach tu Name, vi du Mr, Mrs, Miss, Master
AgeGroup = Child, Teen, Adult, Senior
FareGroup = Low, Medium, High
Deck = ky tu dau cua Cabin hoac Unknown
```

Kiem tra output:

```bash
docker exec -it namenode hdfs dfs -ls /output/titanic/processed
docker exec -it namenode hdfs dfs -cat /output/titanic/processed/part-* | head
```

Output:

```text
hdfs://namenode:9000/output/titanic/processed
```

## 8. Buoc 4 - Thong ke du lieu

Chay:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicStats.py
```

File nay tao cac bang thong ke:

```text
/output/titanic/stats/survival_by_sex
/output/titanic/stats/survival_by_pclass
/output/titanic/stats/survival_by_embarked
/output/titanic/stats/survival_by_age_group
/output/titanic/stats/survival_by_family_size
/output/titanic/stats/survival_by_title
/output/titanic/stats/fare_by_pclass
```

Moi bang thong ke co cac chi so nhu:

```text
total_passengers
survived_count
survival_rate_percent
avg_age
avg_fare
```

Kiem tra:

```bash
docker exec -it namenode hdfs dfs -ls /output/titanic/stats
docker exec -it namenode hdfs dfs -cat /output/titanic/stats/survival_by_sex/part-*
```

## 9. Buoc 5 - Truc quan hoa

Chay:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicVisualize.py
```

File nay doc cac bang thong ke nho, collect ve driver va ve bieu do PNG bang matplotlib.

Anh duoc luu tai:

```text
/data/visualizations
```

Kiem tra:

```bash
docker exec -it spark-master ls /data/visualizations
```

Vi thu muc `data` duoc mount ra ngoai container, anh cung se nam trong:

```text
bigdata-docker-compose/data/visualizations/
```

Cac anh du kien:

```text
survival_by_sex.png
survival_by_pclass.png
survival_by_embarked.png
survival_by_age_group.png
family_size_distribution.png
fare_by_pclass.png
```

Luu y: container can co `matplotlib`. Neu chay `TitanicVisualize.py` bi loi thieu matplotlib, can bo sung vao Dockerfile:

```bash
pip install matplotlib
```

hoac cai bang apt neu image ho tro:

```bash
apt-get install -y python3-matplotlib
```

## 10. Lenh chay nhanh toan bo pipeline

Sau khi da upload 3 file input len HDFS, co the chay lien tiep:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicMerge.py
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicClean.py
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicProcess.py
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicStats.py
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /data/TitanicVisualize.py
```

## 11. Neu muon chay trong PySpark shell

Co the vao container Spark master:

```bash
docker exec -it spark-master bash
```

Mo PySpark shell:

```bash
pyspark --master spark://spark-master:7077
```

Luc nay co the go truc tiep cac lenh PySpark de demo tung buoc. Tuy nhien, khi nop bai van nen nop cac file `.py` vi script de chay lai va kiem chung hon.

## 12. Noi dung mo ta trong bao cao

Co the viet:

```text
Nhom dua ba tap du lieu train.csv, test.csv va gender_submission.csv len HDFS. Sau do, nhom su dung PySpark tren Hadoop Cluster de ghep test.csv voi gender_submission.csv theo PassengerId nham bo sung cot Survived. Tap test da co Survived duoc hop nhat voi train.csv de tao tap merged_titanic. Tu tap du lieu hop nhat, nhom thuc hien lam sach du lieu, xu ly gia tri thieu, chuan hoa kieu du lieu, tao cac dac trung moi nhu FamilySize, IsAlone, Title, AgeGroup, FareGroup va Deck. Cuoi cung, nhom tong hop du lieu bang Spark va truc quan hoa ket qua phan tich thanh cac bieu do PNG.
```

Luu y khi bao cao:

```text
gender_submission.csv duoc su dung de bo sung cot Survived cho tap test.csv phuc vu muc tieu mo phong va phan tich du lieu.
```

