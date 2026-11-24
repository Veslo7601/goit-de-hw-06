from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, 
                               IntegerType, DoubleType)


from config import KAFKA_BOOTSTRAP, KAFKA_USER, KAFKA_PASSWORD, TOPIC_SENSORS, TOPIC_ALERTS

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("hw06_sensor_alerts") \
    .getOrCreate()


JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USER}" '
    f'password="{KAFKA_PASSWORD}";'
)


KAFKA_OPTIONS = {
    'kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
    'kafka.security.protocol': 'SASL_PLAINTEXT',
    'kafka.sasl.mechanism' : 'PLAIN',
    'kafka.sasl.jaas.config': JAAS
}

raw = spark.readStream \
    .format("kafka") \
    .options(**KAFKA_OPTIONS) \
    .option("subscribe", TOPIC_SENSORS) \
    .option("startingOffsets", "latest") \
    .load()

sensor_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])

sensors = raw \
    .select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.col("timestamp").cast("timestamp"))


avg_stats = sensors \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(F.window("timestamp", "1 minute", "30 seconds")) \
    .agg(
        F.round(F.avg("temperature"), 2).alias("t_avg"),
        F.round(F.avg("humidity"), 2).alias("h_avg")
    )


alerts_conditions = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark/work-dir/hw06/alerts_conditions.csv")


def in_range(value_col, min_col, max_col):
    lower_ok = (F.col(min_col) == -999) | (F.col(value_col) >= F.col(min_col))
    upper_ok = (F.col(max_col) == -999) | (F.col(value_col) <= F.col(max_col))
    return lower_ok & upper_ok


alerts = avg_stats.crossJoin(alerts_conditions) \
    .where(
        in_range("t_avg", "temperature_min", "temperature_max")
        & in_range("h_avg", "humidity_min", "humidity_max")
    ) \
    .select(
        "window",
        "t_avg",
        "h_avg",
        F.col("code").cast("string").alias("code"),
        "message",
        F.current_timestamp().alias("timestamp"),
    )

kafka_query = alerts \
    .select(
        F.col("code").alias("key"),
        F.to_json(F.struct("window", "t_avg", "h_avg", "code", "message", "timestamp")).alias("value"),
    ) \
    .writeStream \
    .format("kafka") \
    .options(**KAFKA_OPTIONS) \
    .option("topic", TOPIC_ALERTS) \
    .option("checkpointLocation", "/tmp/checkpoints/hw06_alerts") \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()

console_query = alerts.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started. Ctrl+C to stop.")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping queries...")
finally:
    for q in spark.streams.active:
        q.stop()
    print("Stopped.")