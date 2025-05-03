import scrapy
from scrapy import Request

class MykboSpider(scrapy.Spider):
    name = "kbo_spider"
    allowed_domains = ["mykbostats.com"]
    start_urls = ["https://mykbostats.com/schedule"]
    
    week_count = 1  # Initialize week count
    max_weeks = 3  # Set the maximum number of weeks to scrape
    
    def parse(self, response):
        if response.meta['driver']:
            driver = response.meta['driver']
        
        games = response.css('a.game-line')
        self.logger.info(f"[parse] Found {len(games)} games")
            
        for game in games[:3]:
            middle = game.css('div.middle')
            status_text = game.css('div.status::text').get()
            has_time_class = game.css("div.time")
            if (status_text and "Canceled" in status_text) or has_time_class:
                self.logger.info(f"[parse] Game is cancelled or not played yet, skipping.")
                continue    
            
            game_url = game.attrib['href']
            if game_url:
                url = response.urljoin(game_url)
                self.logger.info(f"[parse] Following game URL: {url}")
                yield Request(url, callback=self.parse_game, meta=response.meta)
        
        # games = response.css('a.game-line::attr(href)').getall()
        # self.logger.info(f"[parse] Found {len(games)} games")
        # for game_href in games:
        #     url = response.urljoin(game_href)
        #     self.logger.info(f"[parse] Following game URL: {url}")
        #     yield Request(url, callback=self.parse_game, meta=response.meta)
        
        # Pagination
        prev_week_href = response.css("a.ui.button[href*='week_of']::attr(href)").get()
        if prev_week_href and self.week_count < self.max_weeks:
            self.week_count += 1
            next_url = response.urljoin(prev_week_href)
            self.logger.info(f"[parse] Following pagination to {next_url}")
            yield Request(url=next_url, callback=self.parse, meta=response.meta)
            
    def parse_game(self, response):
        driver = response.meta['driver']
        
        game_id = response.url.split('/')[-1]  # Extract the game ID (just the number after 'game-line-')
        self.logger.info(f"[parse_game] Game ID: {game_id}")
        
        date = response.css("time::attr(datetime)").get()  # '2025-04-29T09:30:00Z'
        self.logger.info(f"[parse_game] Date: {date}")

        home_team = response.css("table.home thead .team-name::text").get().strip()
        away_team = response.css("table.away thead .team-name::text").get().strip()
        self.logger.info(f"[parse_game] Home Team: {home_team}, Away Team: {away_team}")
        
        scores = response.css("span.score::text").getall()
        away_score = int(scores[0])
        home_score = int(scores[1])
        self.logger.info(f"[parse_game] Away Score: {away_score}, Home Score: {home_score}")
        
        # Batting stats
        away_batting = self.extract_batting_stats(response, 'away')
        home_batting = self.extract_batting_stats(response, 'home')
        
        # Pitching stats
        away_pitching = self.extract_pitching_stats(response, 'away')
        home_pitching = self.extract_pitching_stats(response, 'home')
        
        yield {
            "date": date,
            "away_team": away_team,
            "home_team": home_team,
            "away_score": away_score,
            "home_score": home_score,
            "away_batting": away_batting,
            "home_batting": home_batting,
            "away_pitching": away_pitching,
            "home_pitching": home_pitching,
            "url": response.url,
        }
        
    def extract_batting_stats(self, response, team):
        rows = response.css(f'table.{team} tbody tr')
        players = []
        for row in rows:
            name = row.css('a.player-link::text').get()
            stats = row.css('td.right.aligned::text').getall()
            if len(stats) == 8:
                players.append({
                    "name": name,
                    "BA": stats[0],
                    "AB": stats[1],
                    "R": stats[2],
                    "H": stats[3],
                    "HR": stats[4],
                    "RBI": stats[5],
                    "BB": stats[6],
                    "SO": stats[7],
                    "HBP": row.css("td::text")[-1].get()
                })
        return players
    
    def extract_pitching_stats(self, response, team_type):
        rows = response.css(f"table.{team_type} tbody tr")
        pitchers = []
        for row in rows:
            name = row.css("a.player-link::text").get()
            stats = row.css("td.right.aligned::text").getall()
            if len(stats) >= 10:
                pitchers.append({
                    "name": name,
                    "ERA": stats[0],
                    "IP": stats[1],
                    "NP": stats[2],
                    "R": stats[3],
                    "ER": stats[4],
                    "H": stats[5],
                    "HR": stats[6],
                    "SO": stats[7],
                    "BB": stats[8],
                    "HB": stats[9],
                })
        return pitchers