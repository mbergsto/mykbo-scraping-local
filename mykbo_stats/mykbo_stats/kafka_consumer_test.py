import json
from confluent_kafka import Consumer, KafkaException
import os
from scrapy.utils.project import get_project_settings

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address localhost
    #'bootstrap.servers': get_project_settings().get("KAFKA_BOOTSTRAP_SERVER"), # Kafka broker address on Pi 1
    'group.id': 'kbo-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['kbo_game_data'])

output_file = '../results/consumed_connection_test.jl'

try:
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'a') as f:
        print("Starting Kafka consumer... Press Ctrl+C to stop.")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                data = json.loads(msg.value().decode('utf-8').replace("'", '"'))
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
                print(f"Saved message with key: {msg.key().decode('utf-8')}")
            except Exception as e:
                print(f"Failed to decode message: {e}")
finally:
    consumer.close()
