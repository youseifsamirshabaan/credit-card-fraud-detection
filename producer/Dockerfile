FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=180 --retries 8 -r /app/requirements.txt

COPY producer.py /app/producer.py

CMD ["python", "-u", "producer.py"]
