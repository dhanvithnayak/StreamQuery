FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce_events.py .

ENTRYPOINT ["python", "produce_events.py"]
CMD ["--continuous", "--interval", "1.0"]
