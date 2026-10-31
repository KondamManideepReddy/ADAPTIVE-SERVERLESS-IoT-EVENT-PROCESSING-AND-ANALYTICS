FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt requirements-optional.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-optional.txt
COPY . .
# default: experiment driver (MQTT via Mosquitto, local FaaS emulator, SQLite). Override CMD for the function server:
#   python lambda_fn/server.py 8080
CMD ["python", "run_experiments.py", "--extra", "--transport", "mqtt", "--mqtt-host", "mosquitto", "--prom-port", "8000"]
