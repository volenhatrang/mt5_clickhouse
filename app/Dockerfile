FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y python3 python3-pip python3-venv

RUN python3 -m venv /opt/venv

RUN /opt/venv/bin/pip install --no-cache-dir mt5linux python-dotenv clickhouse-driver esmerald retry requests

ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

COPY . /app

CMD ["python", "download_yah_prices.py"]

# CMD ["tail", "-f", "/dev/null"]

