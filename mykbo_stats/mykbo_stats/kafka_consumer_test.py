import json
from confluent_kafka import Consumer, KafkaException
import os
from scrapy.utils.project import get_project_settings

settings = get_project_settings()

env = get_project_settings().get("RUN_ENV")
if env == "local":
    kafka_bootstrap = settings.get("KAFKA_LOCAL_BOOTSTRAP_SERVER") # Kafka broker address localhost
else:
    kafka_bootstrap = settings.get("KAFKA_BOOTSTRAP_SERVER") # Kafka broker address on Pi 1

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': kafka_bootstrap, 
    'group.id': 'kbo-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance with the specified configuration
consumer = Consumer(consumer_conf)

# Subscribe the consumer to the 'kbo_game_data' topic
consumer.subscribe(['kbo_game_data'])

# Define the output file path for storing consumed messages
output_file = '../results/consumed_data.jl'

try:
    # Ensure the output directory exists, create it if necessary
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Open the output file in append mode
    with open(output_file, 'a') as f:
        print("Starting Kafka consumer... Press Ctrl+C to stop.")
        while True:
            # Poll for new messages from Kafka with a timeout of 1 second
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # No message received, continue polling
            if msg.error():
                # Raise an exception if there is an error in the message
                raise KafkaException(msg.error())
            try:
                # Decode the message value and convert it to JSON
                data = json.loads(msg.value().decode('utf-8').replace("'", '"'))
                # Write the JSON data to the output file
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
                # Print a confirmation message with the message key
                print(f"Saved message with key: {msg.key().decode('utf-8')}")
            except Exception as e:
                # Handle any errors that occur during message decoding
                print(f"Failed to decode message: {e}")
finally:
    # Close the Kafka consumer to release resources
    consumer.close()
