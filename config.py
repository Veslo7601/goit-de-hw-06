import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]

KAFKA_CONFIG = {
    "bootstrap_servers": [KAFKA_BOOTSTRAP],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": KAFKA_USER,
    "sasl_plain_password": KAFKA_PASSWORD,
}

NAME_TOPIC = "ihor"

TOPIC_SENSORS = f"building_sensors_{NAME_TOPIC}"
TOPIC_ALERTS = f"alerts_{NAME_TOPIC}"