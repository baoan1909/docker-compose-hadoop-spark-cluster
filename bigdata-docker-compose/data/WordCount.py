import re
from pyspark import SparkContext, SparkConf

# 1. Khai báo
conf = SparkConf().setAppName("BaoAn_NLP_WordCount_Norvig13MB")
sc = SparkContext(conf=conf)

# 2. Đọc file từ HDFS
text_file = sc.textFile("hdfs://namenode:9000/input/mark_twain_15mb.txt")

# Hàm dọn rác ngôn ngữ tự nhiên
def clean_and_split(line):
    # Hạ hết về chữ thường (Hadoop == hadoop)
    line_lower = line.lower()
    # Dùng Regex gọt sạch mọi ký tự không phải chữ cái và số (xóa chấm, phẩy, ngoặc...)
    clean_text = re.sub(r'[^\w\s]', '', line_lower)
    # Cắt thành mảng các từ
    return clean_text.split()

# 3. Thực thi MapReduce
counts = text_file.flatMap(clean_and_split) \
                  .filter(lambda word: len(word) > 0) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

# 4. Ghi vào thư mục đầu ra mới
output_path = "hdfs://namenode:9000/output_mark_twain"
counts.saveAsTextFile(output_path)

sc.stop()
