# Weather Forecast Dashboard

A real-time weather dashboard for Helsinki using Kafka, Cassandra, Spark, and a Random Forest model for forecasting.

## Stack

- **Kafka** – streams weather data
- **Spark** – ETL processing
- **Cassandra** – stores processed data
- **MLflow** – model tracking
- **Streamlit** – dashboard UI

## How to run

Start Kafka with Docker:
```bash
docker-compose up -d
```

Run the pipeline:
```bash
python weather_producer.py
python weather_consumer.py
python etl_spark.py
python train_model.py
streamlit run app.py
```

## Keep it running (VPS or local)

Use `screen` or `tmux` to keep processes alive after closing the terminal:

```bash
# install if needed
sudo apt install screen

# start each in its own screen session
screen -S producer
python weather_producer.py
# Ctrl+A then D to detach

screen -S consumer
python weather_consumer.py
# Ctrl+A then D to detach
```

To reattach later: `screen -r producer`

Or use `nohup` for a simpler one-liner:
```bash
nohup python weather_producer.py &
nohup python weather_consumer.py &
```

Logs will be written to `nohup.out`.

## Files

| File | Description |
|------|-------------|
| `weather_producer.py` | Fetches weather data and sends to Kafka |
| `weather_consumer.py` | Reads from Kafka and stores to Cassandra |
| `etl_spark.py` | Processes raw data with Spark |
| `train_model.py` | Trains Random Forest model with MLflow |
| `app.py` | Streamlit dashboard |
