# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# Importing necessary modules
# Import selenium for to handle dynamic content before the 
# response is sent to the scrapy spider

from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
import json

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class MykboStatsSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

# Modify the downloader middleware to use Selenium

class MykboStatsDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        
        # Check if the request is for a game page
        self.driver.get(request.url)
        
        print("Processing new request: ", request.url)
        
        # Wait for the page to load and check if it contains game data
        if "/games/" in request.url:
            try:
                # Wait for the game data to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.away tbody tr"))
                )
            except Exception as e:
                spider.logger.warning(f"Timeout or blockin on page-url: {request.url}")
                spider.logger.warning(f"Exception: {e}")
                return HtmlResponse(request.url, status=403, body=b'', encoding='utf-8', request=request)
        
        # If the request is for a schedule page, wait for the schedule to load
        time.sleep(6)
        request.meta['driver'] = self.driver
        # Wait for the page to load
        self.driver.implicitly_wait(5)
        body = str.encode(self.driver.page_source, 'utf-8')
        request.meta['driver_response'] = HtmlResponse(
            url=self.driver.current_url,
            body=body,
            encoding='utf-8',
            request=request)

        return request.meta['driver_response']
        # return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
          
        # Log spider opening
        spider.logger.info("Spider opened: %s" % spider.name)
        
        # Set a custom user agent string
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        
        # Initialize Chrome options for undetected_chromedriver
        options = uc.ChromeOptions()
        # options.add_argument("--headless=new")  # Uncomment for headless mode
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-agent={user_agent}")
        options.add_argument("--start-minimized")
        
        # Start undetected Chrome driver with options
        self.driver = uc.Chrome(options=options, headless=False)
        time.sleep(2)  # Allow time for the browser to start
        
        # Remove 'navigator.webdriver' property to help avoid detection
        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
                });
            """
            }
        )
        
    def spider_closed(self, spider):
            
        self.driver.quit()
        spider.logger.info("Spider closed: %s" % spider.name)
        