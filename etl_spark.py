import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow --add-opens java.base/javax.security.auth=ALL-UNNAMED" pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, lag, avg, col
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName('WeatherETL') \
    .config('spark.cassandra.connection.host', 'localhost') \
    .config('spark.jars.packages',
            'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1') \
    .config('spark.driver.extraJavaOptions',
            '--add-opens java.base/javax.security.auth=ALL-UNNAMED') \
    .config('spark.executor.extraJavaOptions',
            '--add-opens java.base/javax.security.auth=ALL-UNNAMED') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print('Loading data from Cassandra...')
df = spark.read \
    .format('org.apache.spark.sql.cassandra') \
    .options(table='weather_records', keyspace='weather_db') \
    .load()

print(f'Loaded {df.count()} rows')

df = df.orderBy('timestamp')

window = Window.partitionBy('city').orderBy('timestamp')

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