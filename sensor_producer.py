import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from config import TOPIC_SENSORS, KAFKA_CONFIG

SENSOR_ID = random.randint(1000, 9999)

producer = KafkaProducer(
    **KAFKA_CONFIG,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Sensor {SENSOR_ID} started. Ctrl+C to stop.")

try:
    while True:
        data = {
            "sensor_id": SENSOR_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(25, 45), 1),
            "humidity": round(random.uniform(15, 85), 1),
        }
        producer.send(TOPIC_SENSORS, key=SENSOR_ID, value=data)
        print(f"Sent data: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print(f"Sensor {SENSOR_ID} stopped.")
finally:
    producer.flush()
    producer.close()