FROM python:3.12-slim
WORKDIR /app
# solace-pubsubplus for the SMF control/rate/restriction messages; kafka-python to
# produce the wind-shift METAR to the nisshi Kafka on-ramp (Feldera kafka_input).
RUN pip install --no-cache-dir solace-pubsubplus kafka-python==3.0.13
COPY scenario_driver.py /app/scenario_driver.py
CMD ["python", "scenario_driver.py"]
