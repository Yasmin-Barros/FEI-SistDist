FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY python/ /app/

# O comando (broker.py, server.py ou client.py) é definido no docker-compose.yaml
CMD ["python", "broker.py"]
