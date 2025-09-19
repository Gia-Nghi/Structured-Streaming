import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = (SparkSession.builder
         .appName("KafkaSparkStructuredStreamingDemo")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1) Source: Kafka
df = (spark._________.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")  # trong docker network
      .option("subscribe", "store_sales")
      .option("startingOffsets", "latest")
      .load())

# 2) Schema (khớp với producer mới)
schema = StructType([
    StructField("ProductID", StringType()),
    StructField("Category", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("Price", DoubleType()),
    StructField("EventTime", StringType())  # sẽ cast sang Timestamp
])

parsed = (df.selectExpr("CAST(value AS STRING) AS v")
            .select(from_json(col("v"), schema).alias("j"))
            .select("j.*"))

orders = (parsed
          .withColumn("ts", to_timestamp(col("EventTime")))  # Timestamp cho window/watermark
          .withColumn("Revenue", col("Quantity") * col("Price")))

# 3a) Tổng doanh thu theo Category (toàn cục)
by_category = (orders
               ._________("Category")
               ._________(_sum("Revenue").alias("Total_Revenue"))
               )

# 3b) Tổng doanh thu theo cửa sổ 1 phút + watermark 2 phút
by_window = (orders
             .withWatermark("___(1)___", "___(2)___")
             .groupBy(window(col("___(3)___"), "___(4)___"), col("___(5)___"))
             .agg(_sum("___(6)___").alias("Total_Revenue"))
             .select(
                 col("window.___(7)___").alias("window_start"),
                 col("window.___(8)___").alias("window_end"),
                 col("___(9)___"),
                 col("Total_Revenue")
             )
             )
"""
(1) → tên cột timestamp
(2) → độ dài watermark
(3) → tên cột timestamp
(4) → độ dài cửa sổ
(5) → cột để nhóm theo loại sản phẩm
(6) → cột chứa giá trị doanh thu cần tính tổng
(7), (8) → trường bắt đầu/kết thúc cửa sổ
(9) → lại là cột loại sản phẩm
"""

# 4) Sinks
q1 = (by_category._________
      .outputMode("complete")               # bảng tổng hợp toàn cục
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/tmp/spark-checkpoints/store_sales_cat")  # nhớ tách checkpoint
      .start())

q2 = (by_window._________
      .outputMode("update")                 # chỉ cập nhật cửa sổ thay đổi
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/tmp/spark-checkpoints/store_sales_win")
      .start())

spark.streams.awaitAnyTermination()