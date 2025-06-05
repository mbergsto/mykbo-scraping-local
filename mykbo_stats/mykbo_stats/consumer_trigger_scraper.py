from confluent_kafka import Consumer, KafkaException
import subprocess
import logging
import os
from pathlib import Path
from scrapy.utils.project import get_project_settings

settings = get_project_settings()  # Load Scrapy project settings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

kafka_bootstrap = settings.get("KAFKA_BOOTSTRAP_SERVER")  # Kafka broker address

# Kafka consumer configuration
conf = {
    'bootstrap.servers': kafka_bootstrap,
    'group.id': 'trigger-scrape-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)  # Create Kafka consumer
consumer.subscribe(['trigger_scrape'])  # Subscribe to topic

def run_scrapy_spider(spider_name, project_dir):
    # Run a Scrapy spider as a subprocess
    project_path = Path(project_dir).resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_path)

    try:
        subprocess.run(
            ["scrapy", "crawl", spider_name],
            check=True,
            cwd=str(project_path),
            env=env
        )
        logging.info(f"Scrapy spider '{spider_name}' executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Scrapy spider '{spider_name}' failed: {e}")

try:
    logging.info("Listening for scrape trigger messages...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new Kafka messages
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        logging.info("Trigger message received. Running scraper...")
        try:
            project_root = Path(__file__).resolve().parents[1]  # Go to mykbo_stats directory to find Scrapy project
            run_scrapy_spider("kbo_spider", project_dir=project_root)
            
        except Exception as e:
            logging.error(f"Error running Scrapy spider: {e}")

finally:
    consumer.close()  # Close Kafka consumer when done
