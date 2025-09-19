import json, time, datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],   # host → 9093 (theo compose)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'store_sales'
print(f"Bắt đầu gửi dữ liệu từ StoreSales.json tới topic '{topic_name}'...")

def to_iso(dmy: str) -> str:
    # "31-07-2012" -> "2012-07-31T00:00:00Z"
    dt = datetime.datetime.strptime(dmy, "%d-%m-%Y")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

with open("StoreSales.json", "r", encoding="utf-8") as f:
    sales_data = json.load(f)

for record in sales_data:
    qty = int(record["Quantity"]) if record["Quantity"] else 0
    sales = float(record["Sales"]) if record["Sales"] else 0.0
    price = round(sales / qty, 4) if qty else 0.0

    data = {
        "ProductID": record["Product ID"],
        "Category": record["Category"],
        "Quantity": qty,
        "Price": price,                          # <— thêm Price
        "EventTime": to_iso(record["Order Date"])# <— thêm EventTime (ISO)
    }

    producer.send(topic_name, value=data)
    print("Đã gửi:", data)
    time.sleep(0.5)   # mô phỏng stream liên tục

producer.flush()
producer.close()
print("Hoàn tất gửi dữ liệu.")
