import scrapy
from scrapy.loader import ItemLoader
from mykbo_stats.items import GameData, TeamData, BattingStat, PitchingStat
from datetime import datetime
from scrapy.utils.project import get_project_settings
import mariadb

settings = get_project_settings()

env = get_project_settings().get("RUN_ENV")
if env == "local":
    db_params = settings.get("CONNECTION_STRING_LOCAL") # Local DB connection
else:
    db_params = settings.get("CONNECTION_STRING_REMOTE") # Remote DB connection on Pi 2

class MykboSpider(scrapy.Spider):
    name = "kbo_test"
    allowed_domains = ["mykbostats.com"]
    start_urls = ["https://mykbostats.com/schedule"]

    EARLIEST_DATE = datetime(2025, 5, 1)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings = get_project_settings()
        self.check_latest_scrape = settings.get("ENABLE_SCRAPE_DATE_CHECK", True)
        self.latest_scrape_date = None
        if self.check_latest_scrape:
            try:
                self.conn = mariadb.connect(
                    user=db_params["user"],
                    password=db_params["password"],
                    host=db_params["host"],
                    port=db_params["port"],
                    database=db_params["database"]
                )
                self.cursor = self.conn.cursor()
                self.cursor.execute("SELECT MAX(run_timestamp) FROM scrape_runs")
                result = self.cursor.fetchone()
                if result and result[0]:
                    self.latest_scrape_date = result[0]
                    self.conn.close()
            except mariadb.Error as e:
                self.logger.error(f"Error connecting to MariaDB: {e}")
                raise
        self.logger.info(f"[__init__] Latest scrape date: {self.latest_scrape_date}")
        

    def parse(self, response):
        self.logger.info(f"[parse] Parsing schedule page: {response.url}")
        games = response.css('a.game-line')
        stop_pagination = False
        self.logger.info(f"[parse] Found {len(games)} games")
        
        for game in games[:3]:
            date_str = response.css("input#schedule_start::attr(value)").get()
            if date_str:
                date = datetime.strptime(date_str, "%Y-%m-%d")
                if (self.check_latest_scrape and self.latest_scrape_date and date <= self.latest_scrape_date) or date < self.EARLIEST_DATE:
                    stop_pagination = True 
            status_text = game.css('div.status::text').get()
            has_time_class = game.css("div.time")
            if (status_text and "Canceled" in status_text) or has_time_class:
                continue

            game_url = game.attrib['href']
            if game_url:
                url = response.urljoin(game_url)
                yield scrapy.Request(url, callback=self.parse_game)
        
        if stop_pagination:
            self.logger.info(f"[parse] Stopping pagination as the date {date} is earlier than or equal to the latest scrape date {self.latest_scrape_date} or earliest scrape date defined {self.EARLIEST_DATE}.")
            return

        # Pagination
        prev_week_href = response.css("a.ui.button[href*='week_of']::attr(href)").get()
        if prev_week_href:
            next_url = response.urljoin(prev_week_href)
            yield scrapy.Request(next_url, callback=self.parse)

    def parse_game(self, response):
        game_id = response.url.split('/')[-1]
        date = response.css("time::attr(datetime)").get()

        away_team_parts = response.xpath("(//span[@itemprop='name'])[1]//text()").getall()
        home_team_parts = response.xpath("(//span[@itemprop='name'])[2]//text()").getall()
        away_team = " ".join(part.strip() for part in away_team_parts if part.strip())
        home_team = " ".join(part.strip() for part in home_team_parts if part.strip())

        scores = response.css("span.score::text").getall()
        if len(scores) < 2:
            return
        away_score, home_score = scores[0], scores[1]

        away_batting = self.extract_batting_stats(response, "away")
        home_batting = self.extract_batting_stats(response, "home")
        away_pitching = self.extract_pitching_stats(response, "away")
        home_pitching = self.extract_pitching_stats(response, "home")

        away_team_loader = ItemLoader(item=TeamData())
        away_team_loader.add_value("name", away_team)
        away_team_loader.add_value("score", away_score)
        away_team_loader.add_value("batting_stats", away_batting)
        away_team_loader.add_value("pitching_stats", away_pitching)

        home_team_loader = ItemLoader(item=TeamData())
        home_team_loader.add_value("name", home_team)
        home_team_loader.add_value("score", home_score)
        home_team_loader.add_value("batting_stats", home_batting)
        home_team_loader.add_value("pitching_stats", home_pitching)

        game_loader = ItemLoader(item=GameData())
        game_loader.add_value("game_id", game_id)
        game_loader.add_value("date", date)
        game_loader.add_value("teams", {
            "home": home_team_loader.load_item(),
            "away": away_team_loader.load_item()
        })

        yield game_loader.load_item()

    def extract_batting_stats(self, response, team):
        rows = response.css(f'table.{team} tbody tr')
        batters = []
        for row in rows:
            stats = row.css('td.right.aligned::text').getall()
            if len(stats) == 8:
                loader = ItemLoader(item=BattingStat(), selector=row)
                loader.add_css("player_name", "a.player-link::text")
                loader.add_css("number", "span[style*='font-weight: bold'][style*='opacity']::text")
                loader.add_css("position", "td:nth-child(2)::text")
                loader.add_value("at_bats", stats[0])
                loader.add_value("runs", stats[1])
                loader.add_value("hits", stats[2])
                loader.add_value("home_runs", stats[3])
                loader.add_value("runs_batted_in", stats[4])
                loader.add_value("walks", stats[5])
                loader.add_value("strikeouts", stats[6])
                batters.append(loader.load_item())
        return batters

    def extract_pitching_stats(self, response, team):
        rows = response.css(f'table.{team} tbody tr')
        pitchers = []
        for row in rows:
            stats = row.css('td.right.aligned::text').getall()
            if len(stats) >= 9:
                loader = ItemLoader(item=PitchingStat(), selector=row)
                loader.add_css("player_name", "a.player-link::text")
                loader.add_css("number", "span[style*='font-weight: bold'][style*='opacity']::text")
                loader.add_value("innings_pitched", stats[1])
                loader.add_value("pitch_count", stats[2])
                loader.add_value("runs_allowed", stats[3])
                loader.add_value("earned_runs_allowed", stats[4])
                loader.add_value("hits_allowed", stats[5])
                loader.add_value("home_runs_allowed", stats[6])
                loader.add_value("strikeouts", stats[7])
                loader.add_value("walks", stats[8])
                pitchers.append(loader.load_item())
        return pitchers
