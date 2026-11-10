FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir solace-pubsubplus
COPY scenario_driver.py /app/scenario_driver.py
CMD ["python", "scenario_driver.py"]
