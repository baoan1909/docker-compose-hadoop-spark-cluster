import os
import sys

import matplotlib

# Dung backend Agg de ve anh PNG trong moi truong container/headless khong co giao dien.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession


STATS_INPUT = "hdfs://namenode:9000/output/titanic/stats"
VIS_OUTPUT = "/data/visualizations"


def read_stat(spark, stats_base, name):
    # Doc tung bang thong ke da duoc TitanicStats.py ghi ra HDFS.
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{stats_base}/{name}")
    )


def rows_to_lists(df, x_column, y_column):
    # Bang thong ke da nho, co the collect ve driver de ve bang matplotlib.
    rows = df.select(x_column, y_column).collect()
    labels = [str(row[x_column]) for row in rows]
    values = [float(row[y_column]) if row[y_column] is not None else 0.0 for row in rows]
    return labels, values


def save_bar_chart(df, x_column, y_column, title, xlabel, ylabel, output_file):
    # Ve bieu do cot don gian va luu thanh file PNG de dua vao bao cao.
    labels, values = rows_to_lists(df, x_column, y_column)

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
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    plt.close()


def main():
    # Mac dinh doc cac bang stats tren HDFS va luu PNG vao /data/visualizations trong container.
    stats_base = sys.argv[1] if len(sys.argv) > 1 else STATS_INPUT
    output_dir = sys.argv[2] if len(sys.argv) > 2 else VIS_OUTPUT

    os.makedirs(output_dir, exist_ok=True)

    spark = SparkSession.builder.appName("Nhom1_Titanic_Visualize").getOrCreate()

    # Danh sach cac bieu do can tao: ten bang stats, cot truc X, cot truc Y va ten file anh.
    chart_jobs = [
        (
            "survival_by_sex",
            "Sex",
            "survival_rate_percent",
            "Survival Rate by Sex",
            "Sex",
            "Survival rate (%)",
            "survival_by_sex.png",
        ),
        (
            "survival_by_pclass",
            "Pclass",
            "survival_rate_percent",
            "Survival Rate by Passenger Class",
            "Passenger class",
            "Survival rate (%)",
            "survival_by_pclass.png",
        ),
        (
            "survival_by_embarked",
            "Embarked",
            "survival_rate_percent",
            "Survival Rate by Embarked Port",
            "Embarked",
            "Survival rate (%)",
            "survival_by_embarked.png",
        ),
        (
            "survival_by_age_group",
            "AgeGroup",
            "survival_rate_percent",
            "Survival Rate by Age Group",
            "Age group",
            "Survival rate (%)",
            "survival_by_age_group.png",
        ),
        (
            "survival_by_family_size",
            "FamilySize",
            "total_passengers",
            "Passenger Count by Family Size",
            "Family size",
            "Passengers",
            "family_size_distribution.png",
        ),
        (
            "fare_by_pclass",
            "Pclass",
            "avg_fare",
            "Average Fare by Passenger Class",
            "Passenger class",
            "Average fare",
            "fare_by_pclass.png",
        ),
    ]

    # Lap qua tung bang stats, ve bieu do va luu vao thu muc output.
    print("=== TitanicVisualize ===")
    for stat_name, x_col, y_col, title, xlabel, ylabel, filename in chart_jobs:
        stat_df = read_stat(spark, stats_base, stat_name)
        output_file = os.path.join(output_dir, filename)
        save_bar_chart(stat_df, x_col, y_col, title, xlabel, ylabel, output_file)
        print(f"Saved {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
