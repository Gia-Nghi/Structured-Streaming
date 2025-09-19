## **HƯỚNG DẪN CHẠY** 

### **Bước 1: Chuẩn bị và Khởi chạy Môi trường**
1.	Lưu toàn bộ nội dung file cấu hình vào một file tên là docker-compose.yml.
2.	Mở terminal hoặc PowerShell, di chuyển đến thư mục chứa file docker-compose.yml và chạy lệnh sau:
```bash
docker compose up -d
```
 
Lệnh này sẽ tải các image cần thiết và khởi chạy 3 container: kafka, spark-master, và spark-worker ở chế độ nền.
________________________________________
### **Bước 2: Tạo Kafka Topic**
Producer cần một topic để gửi dữ liệu vào. Chúng ta sẽ tạo một topic tên là store_sales (nghĩa là "transaction").
1.	Truy cập vào container kafka đang chạy:
```bash
docker exec -it kafka /bin/bash
``` 
2.	Sau khi đã vào trong container, chạy lệnh sau để tạo topic.
```bash
kafka-topics.sh --create --topic store_sales --bootstrap-server kafka:9092
``` 
3.	Gõ exit để thoát khỏi container Kafka.
 ```bash
exit
```
________________________________________
### **Bước 3: Cài kafka-python**
1.	Đảm bảo bạn đã cài thư viện kafka-python trên máy của mình (không phải trong Docker):
```bash
pip install kafka-python
```
________________________________________
### **Bước 4: Hướng dẫn chạy Demo**
Bây giờ bạn sẽ chạy song song cả producer và consumer.
1.	Chạy Producer (trên máy của bạn): Mở một terminal, di chuyển đến thư mục chứa file producer.py và chạy:
```bash
python producer.py
```
Bạn sẽ thấy các dòng log "Đã gửi: ..." xuất hiện liên tục. Cứ để terminal này chạy.
2.	Chạy Consumer (bên trong Spark container):

  o	Đầu tiên, bạn cần sao chép file spark_consumer.py vào container spark-master:
```bash
docker cp spark_consumer.py spark-master:/opt/bitnami/spark/
```
  o	Mở một terminal mới và thực thi spark-submit. Lệnh này sẽ chạy ứng dụng Spark của bạn và chỉ định các gói thư viện cần thiết để kết nối với Kafka.
```bash
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /opt/bitnami/spark/spark_consumer.py
```
