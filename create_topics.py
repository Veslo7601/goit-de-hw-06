from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import KAFKA_CONFIG, TOPIC_SENSORS, NAME_TOPIC, TOPIC_ALERTS

admin = KafkaAdminClient(**KAFKA_CONFIG)

for name in (TOPIC_SENSORS, TOPIC_ALERTS):
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=2, replication_factor=1)])
        print(f"Topic '{name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists.")

# Перевірка: виводимо лише свої топіки
print("My topics:")
for name in admin.list_topics():
    if NAME_TOPIC in name:
        print(" -", name)

admin.close()    