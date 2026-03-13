import json, time, requests
from kafka import KafkaProducer
from datetime import datetime


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Helsinki coordinates — change to your city
URL = ('https://api.open-meteo.com/v1/forecast'
       '?latitude=60.17&longitude=24.94'
       '&current_weather=true'
       '&hourly=relativehumidity_2m,precipitation_probability')


def fetch_and_send():
    r = requests.get(URL)
    data = r.json()
    current = data['current_weather']
    record = {
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': current['temperature'],
        'windspeed': current['windspeed'],
        'weathercode': current['weathercode'],
    }
    producer.send('weather', record)
    print(f'Sent: {record}')


while True:
    fetch_and_send()
    time.sleep(600)  # wait 10 minutes

