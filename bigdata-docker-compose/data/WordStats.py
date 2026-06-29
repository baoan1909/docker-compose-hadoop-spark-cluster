import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev

# 1. Khởi tạo SparkSession - Cửa ngõ bắt buộc của Spark SQL & DataFrame
spark = SparkSession.builder \
    .appName("BaoAn_Word_Statistics_Day3") \
    .getOrCreate()

sc = spark.sparkContext

# 2. Đọc dữ liệu thô từ HDFS thông qua RDD để xử lý văn bản bằng Regex cũ
text_file = sc.textFile("hdfs://namenode:9000/input/mark_twain_15mb.txt")

def clean_and_split(line):
    line_lower = line.lower()
    clean_text = re.sub(r'[^\w\s]', '', line_lower)
    return clean_text.split()

# Phẳng hóa văn bản thành danh sách toàn bộ các từ hợp lệ
words_rdd = text_file.flatMap(clean_and_split).filter(lambda w: len(w) > 0)

# 3. CHUYỂN ĐỔI SANG DATAFRAME: Biến mỗi từ thành một dòng chứa ĐỘ DÀI (Length) của nó
# Cú pháp chuyển đổi từ RDD Tuple sang DataFrame
df = words_rdd.map(lambda word: (len(word),)).toDF(["word_length"])

# 4. TÍNH TOÁN MEAN VÀ STANDARD DEVIATION (SD)
# Sử dụng các hàm thống kê tích hợp sẵn cực mạnh của Spark SQL
stats = df.select(
    avg("word_length").alias("mean"),
    stddev("word_length").alias("sd")
).collect()[0]

word_mean = stats["mean"]
word_sd = stats["sd"]

# 5. GIẢI QUYẾT NÚT THẮT CỔ CHAI: TÍNH MEDIAN (TRUNG VỊ) XẤP XỈ
# Sử dụng thuật toán Greenwald-Khanna với sai số cho phép là 1% (0.01)
# approxQuantile(column, [probabilities], relativeError)
median_list = df.approxQuantile("word_length", [0.5], 0.01)
word_median = median_list[0] if median_list else 0.0

# 6. ĐÓNG GÓI KẾT QUẢ VÀ IN RA LOG
result_summary = (
    f"=== BAO AN BIG DATA REPORT ===\n"
    f"WordMean (Trung binh): {word_mean:.4f}\n"
    f"WordMedian (Trung vi xap xi): {word_median:.1f}\n"
    f"WordSD (Do lech chuan): {word_sd:.4f}\n"
    f"==============================\n"
)

print("\n" + "="*40)
print(result_summary)
print("="*40 + "\n")

# 7. GHI KẾT QUẢ VÀO HDFS KHÔNG CẦN CHÈN LỆNH XOÁ TỰ ĐỘNG
result_rdd = sc.parallelize([result_summary])
result_rdd.saveAsTextFile("hdfs://namenode:9000/output_word_stats")

# Giải phóng tài nguyên cụm
spark.stop()