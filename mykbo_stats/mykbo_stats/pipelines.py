# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import json
from datetime import datetime

import mariadb
from confluent_kafka import Producer
from itemadapter import ItemAdapter
from scrapy.exceptions import CloseSpider
from scrapy.utils.project import get_project_settings


class ScrapeLogPipeline:
    def __init__(self):
        logging.info("Initializing ScrapeLogPipeline")
        db_params = get_project_settings().get("CONNECTION_STRING_LOCAL")  # Local DB connection
        #db_params = get_project_settings().get("CONNECTION_STRING_REMOTE")  # Remote DB connection on Pi 2
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
        logging.info("Connected to MariaDB")
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
        logging.info("Tables created or already exist")
        self.conn.commit()
        self.current_run_id = None
        self.scraped_dates = set()
        
    def open_spider(self, spider):
        self.cursor.execute("""
            INSERT INTO scrape_runs (latest_game_date) VALUES (NULL)
        """)
        self.conn.commit()
        self.current_run_id = self.cursor.lastrowid
        
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
            return item
        
        self.cursor.execute("""
            INSERT INTO game_metadata (game_id, game_date, scrape_run_id) VALUES (?, ?, ?)
            """, (game_id, game_date, self.current_run_id))
        
        self.conn.commit()
        
        return item
    
    def close_spider(self, spider):
        if self.scraped_dates:
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
            'bootstrap.servers': 'localhost:9092',  # Kafka broker address localhost
            #'bootstrap.servers': get_project_settings().get("KAFKA_BOOTSTRAP_SERVER"), # Kafka broker address on Pi 1

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
        # Ensure all messages are sent before closing the producer
        self.producer.flush()
        

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")