FROM ubuntu:20.04

# Cài đặt Python và các công cụ cần thiết
RUN apt update && apt install -y python3 python3-pip python3-venv

# Tạo virtual environment
RUN python3 -m venv /opt/venv

# Thêm virtual environment vào PATH
ENV PATH="/opt/venv/bin:$PATH"

# Cài đặt các thư viện cơ bản
RUN /opt/venv/bin/pip install --no-cache-dir mt5linux python-dotenv clickhouse-driver esmerald retry requests

# Sao chép file requirements.txt để cài đặt dependencies
COPY ./app/requirements.txt /app/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /app/requirements.txt

# Sao chép toàn bộ mã nguồn sau cùng
WORKDIR /app
COPY ./app /app

# Lệnh chạy chính
CMD ["python", "main.py"]
