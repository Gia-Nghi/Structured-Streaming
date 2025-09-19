import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Khởi tạo SparkSession
spark = SparkSession \
    .builder \
    .appName("KafkaSparkStructuredStreamingDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Tạo streaming DataFrame (Đọc dữ liệu từ Kafka)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "store_sales") \
    .load()

# Định nghĩa schema
schema = StructType([
    StructField("ProductID", StringType()),
    StructField("Category", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("Sales", DoubleType()),
])

# 3. Thao tác trên streaming DataFrame (Parse JSON và xử lý)
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Tính doanh thu = Quantity * Sales theo từng Category
result_df = parsed_df \
    .withColumn("Revenue", col("Quantity") * col("Sales")) \
    .groupBy("Category") \
    .agg(sum("Revenue").alias("Total_Revenue"))

# 4. Khởi động truy vấn trực tuyến (xuất kết quả ra console)
query = result_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/store_sales") \
    .start()

# 5. Quản lý truy vấn + 6. Theo dõi truy vấn
print("=== Query Status ===")
print(query.status)          # Thông tin trạng thái hiện tại
print("=== Query Last Progress ===")
print(query.lastProgress)    # Thông tin lần cập nhật gần nhất

# Chạy liên tục cho đến khi stop
query.awaitTermination()

