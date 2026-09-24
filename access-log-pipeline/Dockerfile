FROM python:3.11-slim

WORKDIR /pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/pipeline

CMD ["sleep", "infinity"]
