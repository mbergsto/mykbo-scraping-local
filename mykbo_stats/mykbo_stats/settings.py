# Scrapy settings for mykbo_stats project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
from datetime import datetime

BOT_NAME = "mykbo_stats"

SPIDER_MODULES = ["mykbo_stats.spiders"]
NEWSPIDER_MODULE = "mykbo_stats.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "mykbo_stats (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

LOG_ENABLED = True
LOG_LEVEL = 'INFO'  # or 'DEBUG' for more detailed logs
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_FILE = os.path.join(log_dir, f'scrapy_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log')


# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "mykbo_stats.middlewares.MykboStatsSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   "mykbo_stats.middlewares.MykboStatsDownloaderMiddleware": 543,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "mykbo_stats.pipelines.ScrapeLogPipeline": 300,
   "mykbo_stats.pipelines.KafkaProducerPipeline": 400,
}

RUN_ENV = "remote"  # "local" or "remote"

CONNECTION_STRING_REMOTE = {
    "user": "bigdata",
    "password": "bigdata+",
    "host": "192.168.1.132",  # IP to mariadb on Pi 2
    "port": 3306,
    "database": "scraping_db"
}

# MariaDB Connection String Local
CONNECTION_STRING_LOCAL = {
    #"driver": "mariadb",
    "user": "bigdata",
    "password": "bigdata+",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "scraping_local",
}

KAFKA_LOCAL_BOOTSTRAP_SERVER = "localhost:9092"  # IP to Kafka Broker localhost

KAFKA_BOOTSTRAP_SERVER = "172.21.229.182"  # IP to Kafka Broker on Pi 1

# Determine if we should check the scrape date to avoid scraping the same date again
ENABLE_SCRAPE_DATE_CHECK = True

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

FEED_EXPORT_INDENT = 2
FEED_FORMAT = "json"
