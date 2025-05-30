# MYKBO-SCRAPING-LOCAL

A Scrapy-based project for scraping KBO (Korean Baseball Organization) game data, with support for Kafka publishing and MariaDB storage.

## Overview

### Spiders

- **`kbo_spider.py`**  
  Main production spider that scrapes full KBO game data.

- **`kbo_test.py`**  
  Spider for local testing and scraping validation.

### Pipelines

- **`ScrapeLogPipeline`**  
  Logs each scraping run and saves game metadata to MariaDB.  
  Automatically creates necessary database tables.

- **`KafkaProducerPipeline`**  
  Publishes each scraped item as a JSON message to the Kafka topic `kbo_game_data`.

### Middleware

- Custom middleware components for modifying Scrapy request/response handling.  
  Can be activated via the `DOWNLOADER_MIDDLEWARES` setting.

### Settings

- Controlled via the `RUN_ENV` variable in `.env` (`local` or `remote`).
- Manages database connections, Kafka brokers, and logging configuration.

### Kafka Consumer Integration

- A Kafka consumer can trigger the scraper by sending a message to the `trigger_scrape` topic.
- The project includes a consumer script that listens and `runs scrapy crawl spider_name` when triggered.

### Results

- Scraped output is saved in the `mykbo_stats/results/` directory in `.jl` or `.json` format.

### Logging

- Scrapy logs are stored in the `mykbo_stats/logs/` folder.  
  Configurable via the `LOG_FILE` setting in `settings.py`.

## Getting Started

1. **Create a virtual environment and activate it**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate

   ```

2. **Install Dependencies**

   ```bash
     pip install -r requirements.txt
   ```

3. **Set up `.env` file**

- Include necessary environment settings.

- For local MariaDB connection

  ```env
    MARIADB_USER=bigdata
    MARIADB_PASSWORD=bigdata+
    MARIADB_DATABASE=scraping_local
    MARIADB_ROOT_PASSWORD=bigdata+
  ```

4. **Ensure required Docker containers are running**

- Either:

  ```bash
  docker-compose -f docker-compose.kafka.yml up -d
  docker-compose -f docker-compose.mariadb.yml up -d
  ```

- Or verify remote services are reachable.

5. **Run the spider**

- This scrapes full KBO game data and stores output under results/:

  ```bash
  scrapy crawl kbo_spider -O results/data.jl
  ```

6. **Optional: Run test spider**

- For testing connections, selectors and logic:

  ```bash
  scrapy crawl kbo_test
  ```

## Kafka Consumer

To trigger the scraper via Kafka, run the consumer script:

```bash
python3 mykbo_stats/consumer_trigger_scraper.py
```

This will listen for messages on the `trigger_scrape` topic and execute the specified spider when a message is received.
The messages are sent from a Kafka producer connected to the web application.
