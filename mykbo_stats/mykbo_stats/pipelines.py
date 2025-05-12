# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
import mariadb
from datetime import datetime
import logging


class ScrapeLogPipeline:
    def __init__(self):
        logging.info("Initializing ScrapeLogPipeline")
        db_params = get_project_settings().get("CONNECTION_STRING_LOCAL")
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
