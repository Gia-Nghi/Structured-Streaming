import json
import time
from kafka import KafkaProducer

# Kết nối đến Kafka server (ngoài Docker dùng port 9093)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'store_sales'
print(f"Bắt đầu gửi dữ liệu từ StoreSales.json tới topic '{topic_name}'...")

# Đọc dữ liệu từ file JSON
with open("StoreSales.json", "r", encoding="utf-8") as f:
    sales_data = json.load(f)

# Gửi từng bản ghi vào Kafka
for record in sales_data:
    # Trích xuất các trường cần thiết
    data = {
        "ProductID": record["Product ID"],
        "Category": record["Category"],
        "Quantity": int(record["Quantity"]),
        "Sales": float(record["Sales"])
    }

    # Gửi message tới Kafka
    producer.send(topic_name, value=data)
    print(f"Đã gửi: {data}")

    time.sleep(1)  # gửi chậm lại để mô phỏng luồng dữ liệu

producer.flush()
producer.close()
print("Hoàn tất gửi dữ liệu.")
