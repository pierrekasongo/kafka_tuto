from kafka import KafkaProducer
import json
import time

from data import get_registered_user

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer = json_serializer)


if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        #Send to specific topic (topic name should match the broker)
        producer.send("register_user", registered_user)
        time.sleep(4)