FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_to_warehouse.py .

ENTRYPOINT ["python", "load_to_warehouse.py"]
CMD ["--daemon", "--poll-timeout", "2.0"]
