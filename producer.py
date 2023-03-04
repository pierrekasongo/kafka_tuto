from kafka import KafkaProducer
import json
import time

from data import get_registered_user

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def get_partition(key, all, available):
    return 0 #Publish only to partition 0

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer = json_serializer,
                            #Uncomment this to publish to specific partition
                            #partitioner=get_partition
                        )


if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        #Send to specific topic (topic name should match the broker)
        producer.send("registered_user", registered_user)
        time.sleep(4)