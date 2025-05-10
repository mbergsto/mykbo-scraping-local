# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.utils.project import get_project_settings
import mariadb
from datetime import datetime


class ScrapeLogPipeline:
    def __init__(self):
        db_params = get_project_settings().get("CONNECTION_STRING_LOCAL")
        self.conn = mariadb.connect(
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
            database=db_params["database"]
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS scrape_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                game_id VARCHAR(255),
                game_date DATETIME,
                scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        game_id = adapter.get("game_id")
        game_date = adapter.get("date")
        
        self.cursor.execute("""
            INSERT INTO scrape_log (game_id, game_date, scraped_at)
            VALUES (?, ?, ?)
        """, (game_id, game_date, datetime.now()))
        self.conn.commit()
        
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
