from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "registered_user", #Topic to listen to
        bootstrap_servers = "localhost:9092",
        auto_offset_reset = "earliest",
        group_id="consumer-group-a"
    )
    print("Starting the consumer")

    for msg in consumer:
        print("Registered user = {}".format(json.loads(msg.value)))