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
from scrapy.exceptions import CloseSpider
from scrapy.utils.project import get_project_settings

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
        # Initialize the pipeline and set up database connection
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
        logging.info("Connected to MariaDB")
        self.cursor = self.conn.cursor()
        
        # Create tables if they do not already exist
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
        self.current_run_id = None  # ID of the current scrape run
        self.scraped_dates = set()  # Set to track scraped game dates
        
    def open_spider(self, spider):
        # Insert a new scrape run entry when the spider starts
        korea_time = datetime.now(pytz.timezone("Asia/Seoul"))
        self.cursor.execute("""
            INSERT INTO scrape_runs (run_timestamp, latest_game_date) VALUES (?, NULL)
        """, (korea_time,))
        self.conn.commit()
        self.current_run_id = self.cursor.lastrowid  # Store the ID of the current scrape run
        
    def process_item(self, item, spider):
        # Process each scraped item and insert it into the database
        adapter = ItemAdapter(item)
        game_id = adapter.get("game_id")
        game_date = adapter.get("date")
        
        self.scraped_dates.add(game_date)  # Track the date of the scraped game
        
        # Check if the game already exists in the database
        self.cursor.execute("""
            SELECT id FROM game_metadata WHERE game_id = ?
            """, (game_id,))
        if self.cursor.fetchone():
            spider.logger.info(f"Game {game_id} already exists in the database. Skipping.")
            return item
        
        # Insert new game metadata into the database
        self.cursor.execute("""
            INSERT INTO game_metadata (game_id, game_date, scrape_run_id) VALUES (?, ?, ?)
            """, (game_id, game_date, self.current_run_id))
        
        self.conn.commit()
        
        return item
    
    def close_spider(self, spider):
        # Update the latest game date for the scrape run and close the database connection
        if self.scraped_dates:
            latest_date = max(self.scraped_dates)  # Determine the latest game date
            self.cursor.execute("""
                UPDATE scrape_runs SET latest_game_date = ? WHERE id = ?
            """, (latest_date, self.current_run_id))
            self.conn.commit()
        self.cursor.close()  # Close the database cursor
        self.conn.close()  # Close the database connection
        
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
        # Ensure all messages are sent before closing the producer
        self.producer.flush()
        

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")