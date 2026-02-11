FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY src/ /app/src/

RUN pip install --no-cache-dir -r /app/src/requirements.txt

ENV PYTHONUNBUFFERED=1
ENV CDC_DB_HOST=mysql
ENV CDC_DB_PORT=3306
ENV CDC_DB_USER=root
ENV CDC_DB_PASSWORD=root_password
ENV CDC_DB_NAME=cdc_db
ENV CDC_TABLE_NAME=products
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=cdc_events
ENV POLLING_INTERVAL=5

CMD ["python", "-m", "src.main"]
