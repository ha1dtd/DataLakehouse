# Hướng dẫn Vận hành Pipeline Data: Từ Setup đến Airflow

Tài liệu này hướng dẫn cách đưa một cụm (cluster) vừa cài đặt xong vào trạng thái hoạt động thực tế với Airflow và xử lý dữ liệu qua các tầng Bronze -> Silver -> Gold.

---

## 1. Khởi động Cluster & Các dịch vụ lõi

Sau khi chạy xong 2 file `setup_namenode.sh` và `setup_datanode.sh`, cluster đã có sẵn các file cài đặt nhưng bạn cần bật chúng lên. 

Tất cả các lệnh dưới đây đều chạy trên máy **Namenode**:

### Bước 1.1: Bật HDFS (Hệ thống lưu trữ file phân tán)
Chạy lệnh sau để khởi động Namenode và kết nối với các Datanode:
```bash
start-dfs.sh
```

### Bước 1.2: Bật YARN (Hệ thống quản lý tài nguyên tính toán cho Spark)
Chạy lệnh sau để khởi động ResourceManager và NodeManagers:
```bash
start-yarn.sh
```

*(Kiểm tra bằng lệnh `jps` trên Namenode. Bạn phải thấy các tiến trình: NameNode, SecondaryNameNode, ResourceManager).*

### Bước 1.3: Đảm bảo MinIO (Object Storage) đang chạy
MinIO đóng vai trò là nơi lưu trữ dữ liệu chính cho các tầng Bronze, Silver, Gold thay vì dùng HDFS truyền thống. Script setup đã tự động cài đặt MinIO, nhưng để chắc chắn nó đang chạy, bạn kiểm tra bằng lệnh:
```bash
pgrep -f "minio server"
```
*(Nếu không có output, bạn cần bật lại MinIO theo lệnh trong script: `nohup minio server ~/minio-data --address ":9001" --console-address ":9002" > ~/minio.log 2>&1 &`)*

---

## 2. Truy cập các giao diện quản lý (Tunneling)

Vì cluster nằm trên các máy ảo (VM) có Private IP, bạn cần mở tunnel (cổng nối) từ máy tính cá nhân (laptop của bạn) vào máy Namenode để xem giao diện web.

Trên **laptop của bạn**, mở Terminal và chạy lệnh SSH Tunnel:
```bash
ssh -L 8088:localhost:8088 -L 9002:localhost:9002 -L 8080:localhost:8080 ubuntu@<Public_IP_của_Namenode>
```
- Port `8088`: Giao diện quản lý YARN (xem danh sách jobs).
- Port `9002`: Giao diện web của MinIO (quản lý file lưu trữ).
- Port `8080`: Giao diện web của Airflow (quản lý luồng chạy tự động).

---

## 3. Cấu hình MinIO (Tạo Buckets)

Truy cập MinIO web tại: `http://localhost:9002`
- **Username**: `admin`
- **Password**: `12345678`

Bạn cần vào giao diện tạo sẵn **3 Buckets** sau (tương ứng với 3 tầng dữ liệu):
1. `bronze`
2. `silver`
3. `gold`

*(Bạn chỉ cần tạo tên bucket, không cần cấu hình gì thêm).*

---

## 4. Chạy Airflow & Tải mã nguồn

### Bước 4.1: Bật Airflow
Trên **Namenode**, bật môi trường ảo và khởi động Airflow ở chế độ chạy nền (background):
```bash
source ~/airflow-venv/bin/activate

# Bật Scheduler (Bộ lập lịch)
airflow scheduler -D

# Bật Webserver (Giao diện web)
airflow webserver -p 8080 -D
```

### Bước 4.2: Tải code (DAG & Jobs) vào đúng thư mục
Để Airflow nhận diện được luồng chạy (DAG), file `airflow.py` phải được đặt vào thư mục dags của Airflow. Các file job Python cần được đặt ở thư mục code.

Trên Namenode:
```bash
# Tạo thư mục chứa DAG
mkdir -p ~/airflow/dags
mkdir -p ~/scripts
mkdir -p ~/data

# Bạn cần copy/upload các file từ laptop vào thư mục tương ứng trên Namenode:
# 1. Copy file airflow.py vào thư mục ~/airflow/dags/
# 2. Copy bronze_to_silver.py, silver_to_gold.py, read_bronze.py vào ~/scripts/
# 3. Chép dữ liệu gốc (yellow_tripdata_2025-1.parquet, taxi_zone_lookup.csv) vào ~/data/
```

---

## 5. Giải thích Pipeline (Các tầng dữ liệu & Điều kiện lọc)

Pipeline này chạy tự động bằng Airflow, theo kiến trúc Medallion (Đồng -> Bạc -> Vàng) dùng Spark Iceberg.

### Job 1: Ingestion (Tải dữ liệu)
- **Hành động**: Đẩy file dữ liệu thô (taxi parquet & file csv thông tin khu vực) từ máy chủ lên bucket `bronze` trong MinIO.
- **Output**: Dữ liệu nằm ở `s3://bronze/raw/...` không thay đổi gì so với bản gốc.

### Job 2: Bronze to Silver (Làm sạch & Bổ sung dữ liệu)
- **Hành động**: Đọc dữ liệu từ tầng `bronze`.
- **Tiêu chí làm sạch (Filtering Criteria)**:
  - Loại bỏ hoàn toàn các dòng chứa giá trị `NULL`.
  - Loại bỏ các dòng trùng lặp (Duplicates).
  - Loại bỏ các dòng có giá trị tài chính hoặc khoảng cách **bị âm** (tiền xe >= 0, tiền tip >= 0, v.v.).
  - Tính toán thời gian di chuyển (Trip Duration) tính bằng phút.
  - Loại bỏ các chuyến đi có **thời gian <= 0**.
- **Làm giàu dữ liệu (Enrichment)**: Kết nối (join) mã ID của điểm đón/trả khách với file CSV để lấy ra tên Quận (Borough) và Tên Khu vực (Zone).
- **Output**: Ghi thành bảng Iceberg đã sạch sẽ, chuẩn hoá tại `silver_catalog.default.yellow_taxi`.

### Job 3: Silver to Gold (Tính toán chỉ số nghiệp vụ)
- **Hành động**: Đọc dữ liệu sạch từ tầng `silver`. Tầng này không lọc bỏ dòng nào nữa, mà chỉ tập trung tính toán chỉ số để phục vụ làm báo cáo/phân tích.
- **Biến đổi**:
  - Tạo bảng **taxi_tips**: Phân tích tiền Tip (tính % tip so với giá cước, xác định xem khách có tip hay không).
  - Tạo bảng **taxi_performance**: Phân tích hiệu suất (tính Vận tốc trung bình, Doanh thu trên mỗi phút).
  - Tạo bảng **taxi_financials**: Phân tích tài chính (Tổng phí các loại, Cước trên mỗi dặm, Giá cước trên mỗi hành khách).
  - Tạo bảng **taxi_classifications**: Phân loại chuyến đi (Chuyến vào giờ cao điểm, Chuyến cuối tuần, Phân loại ngắn/trung/dài, Gắn cờ các chuyến đi đáng ngờ).
- **Output**: Tạo ra 4 bảng Iceberg riêng biệt phục vụ mục đích phân tích khác nhau nằm ở `gold_catalog.default.*`.

---

## 6. Chạy và theo dõi Airflow

1. Truy cập Airflow web tại: `http://localhost:8080` (thông qua Tunnel).
2. Đăng nhập bằng user: `admin` / pass: `admin`.
3. Tại trang chủ, tìm DAG có tên là `spark_minio_medallion_pipeline`.
4. Bật công tắc (Toggle) bên cạnh tên DAG từ "Off" sang **"On"**.
5. Bấm nút **"Trigger DAG"** (nút hình mũi tên Play) góc trên bên phải để chạy ngay lập tức.
6. Bấm vào tên DAG, chuyển sang tab **"Graph"** hoặc **"Grid"** để xem các khối vuông chuyển màu:
   - Màu xanh lá nhạt: Đang chạy.
   - Màu xanh lá đậm: Thành công.
   - Màu đỏ: Thất bại.

Trường hợp job thất bại hoặc chạy lâu (stuck ở YARN), hãy mở port `8088` (YARN ResourceManager) để kiểm tra log chi tiết của tiến trình Spark.