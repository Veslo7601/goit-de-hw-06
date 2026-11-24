import json
from kafka import KafkaConsumer
from config import KAFKA_CONFIG, TOPIC_ALERTS, NAME_TOPIC

consumer = KafkaConsumer(
    TOPIC_ALERTS,
    **KAFKA_CONFIG,
    group_id=f"hw06_alerts_viewer_{NAME_TOPIC}",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Listening '{TOPIC_ALERTS}'. Ctrl+C to stop.")

try:
    for message in consumer:
        a = message.value
        w = a["window"]
        print(f"[{a['code']}] {a['message']:<15} | window {w['start']} → {w['end']} "
              f"| t_avg={a['t_avg']} h_avg={a['h_avg']}")
except KeyboardInterrupt:
    print("Stopped.")
finally:
    consumer.close()
