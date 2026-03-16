from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, lag, avg
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName('WeatherETL') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print('Loading data from CSV...')
df = spark.read.csv('raw_weather.csv', header=True, inferSchema=True)

print(f'Loaded {df.count()} rows')
df = df.orderBy('timestamp')

window = Window.orderBy('timestamp')

df = df \
    .withColumn('hour_of_day', hour('timestamp')) \
    .withColumn('day_of_week', dayofweek('timestamp')) \
    .withColumn('temp_lag_1h', lag('temperature', 1).over(window)) \
    .withColumn('temp_rolling_avg',
                avg('temperature').over(window.rowsBetween(-5, 0)))

df = df.dropna()
print(f'Rows after cleaning: {df.count()}')

df.toPandas().to_csv('processed_weather.csv', index=False)
print('ETL complete! Saved to processed_weather.csv')

spark.stop()