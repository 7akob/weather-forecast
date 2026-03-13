import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime


consumer = KafkaConsumer(
    'weather',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


cluster = Cluster(['localhost'])
session = cluster.connect('weather_db')


INSERT_QUERY = session.prepare(
    'INSERT INTO weather_records (city, timestamp, temperature, windspeed, weathercode)'
    'VALUES (?, ?, ?, ?, ?)'
)


for message in consumer:
    d = message.value
    session.execute(INSERT_QUERY, [
        'Helsinki',
        datetime.fromisoformat(d['timestamp']),
        d['temperature'],
        d['windspeed'],
        d['weathercode']
    ])
    print(f"Saved: {d['timestamp']} — {d['temperature']}°C")

