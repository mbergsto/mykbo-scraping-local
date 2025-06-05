# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import json
from datetime import datetime
import pytz

import mariadb
from confluent_kafka import Producer
from itemadapter import ItemAdapter
from scrapy.exceptions import CloseSpider, DropItem
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import logger


settings = get_project_settings()

env = get_project_settings().get("RUN_ENV")
if env == "local":
    db_params = settings.get("CONNECTION_STRING_LOCAL") # Local DB connection
    kafka_bootstrap = settings.get("KAFKA_LOCAL_BOOTSTRAP_SERVER") # Kafka broker address localhost
else:
    db_params = settings.get("CONNECTION_STRING_REMOTE") # Remote DB connection on Pi 2
    kafka_bootstrap = settings.get("KAFKA_BOOTSTRAP_SERVER") # Kafka broker address on Pi 1

class ScrapeLogPipeline:
    def __init__(self):
        logging.info("Initializing ScrapeLogPipeline")
        try:
            self.conn = mariadb.connect(
                user=db_params["user"],
                password=db_params["password"],
                host=db_params["host"],
                port=db_params["port"],
                database=db_params["database"]
            )
        except mariadb.Error as e:
            logging.error(f"Error connecting to MariaDB: {e}")
            raise
        self.cursor = self.conn.cursor()

        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    latest_game_date DATE
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS game_metadata (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    game_id VARCHAR(255) UNIQUE,
                    game_date DATE,
                    scrape_run_id INT,
                    FOREIGN KEY (scrape_run_id) REFERENCES scrape_runs(id)
                )
            """)
        except mariadb.Error as e:
            logging.error(f"Error creating tables: {e}")
            raise
        self.conn.commit()
        self.scraped_dates = set()
        self.current_run_id = None

    def open_spider(self, spider):
        self.scraped_dates.clear()
        self.current_run_id = None

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        game_id = adapter.get("game_id")
        game_date = adapter.get("date")
        self.scraped_dates.add(game_date)

        self.cursor.execute("""
            SELECT id FROM game_metadata WHERE game_id = ?
        """, (game_id,))
        if self.cursor.fetchone():
            spider.logger.info(f"Game {game_id} already exists in the database. Skipping.")
            print(f"Game {game_id} already exists in the database. Skipping.")
            raise DropItem(f"Duplicate game {game_id} – not sending to Kafka.")

        if self.current_run_id is None:
            korea_time = datetime.now(pytz.timezone("Asia/Seoul"))
            self.cursor.execute("""
                INSERT INTO scrape_runs (run_timestamp, latest_game_date)
                VALUES (?, NULL)
            """, (korea_time,))
            self.conn.commit()
            self.current_run_id = self.cursor.lastrowid

        self.cursor.execute("""
            INSERT INTO game_metadata (game_id, game_date, scrape_run_id)
            VALUES (?, ?, ?)
        """, (game_id, game_date, self.current_run_id))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        if self.current_run_id and self.scraped_dates:
            latest_date = max(self.scraped_dates)
            self.cursor.execute("""
                UPDATE scrape_runs SET latest_game_date = ? WHERE id = ?
            """, (latest_date, self.current_run_id))
            self.conn.commit()
        self.cursor.close()
        self.conn.close()
        
class KafkaProducerPipeline:
    def __init__(self):
        # Initialize Kafka producer with server and client configurations
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap,  # Kafka broker address
            'client.id': 'scrapy-producer'         # Client identifier
        })
    
        
    def process_item(self, item, spider):
        # Convert the Scrapy item to a dictionary and serialize it to JSON
        adapter = ItemAdapter(item)
        game_id = adapter.get("game_id")  # Extract game ID to use as the Kafka message key
        
        try:
            value = json.dumps(adapter.asdict(), default=default_serializer)    # Serialize item data to JSON
            self.producer.produce("kbo_game_data", key=game_id, value=value)     # Produce the item to the Kafka topic 'kbo_game_data'
            self.producer.poll(0)  # Trigger delivery of any pending messages
        except Exception as e:
            spider.logger.error(f"Kafka production failed for game {game_id}: {e}")
        return item     # Return the item for further processing in the pipeline
        
    
    def close_spider(self, spider):        
        item_count = spider.crawler.stats.get_value('item_scraped_count', 0)
        if item_count and item_count > 0:
        # Send final control message to indicate the end of the scrape, if there are items scraped
        # This is to avoid sending a control message if no items were scraped
            control_message = json.dumps({
                "type": "scrape_end",
                "timestamp": datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
            })
            self.producer.produce("kbo_game_data", key="control", value= control_message)
            self.producer.poll(0)
            logger.info("Sent control message to Hadoop Consumer indicating end of scrape.")  # For logging
            print("Sent control message to Hadoop Consumer indicating end of scrape.")        # For console output
        else: 
            control_message = json.dumps({
                "type": "no_items_scraped",
                "timestamp": datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
            })
            self.producer.produce("frontend_control", key="control", value=control_message)
            self.producer.poll(0)
            logger.warning("Spider closed without scraping any items. Sent control message to Webapp.") 
            print("Spider closed without scraping any items. Sent control message to Webapp.")
        # Flush the producer to ensure all messages are sent before closing
        self.producer.flush()
        

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")