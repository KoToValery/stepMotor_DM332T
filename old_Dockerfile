FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    i2c-tools \
    kmod \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    smbus2 \
    requests \
    paho-mqtt

COPY run.py /
CMD ["python3", "-u", "/run.py"]
