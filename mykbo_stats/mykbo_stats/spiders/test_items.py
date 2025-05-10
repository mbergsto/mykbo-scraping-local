import scrapy
from scrapy.loader import ItemLoader
from mykbo_stats.items import GameData, TeamData, BattingStat, PitchingStat


class MykboSpider(scrapy.Spider):
    name = "test_items_spider"
    allowed_domains = ["mykbostats.com"]
    start_urls = ["https://mykbostats.com/games/12483-KT-vs-Doosan-20250429"]

    def parse(self, response):
        # Extract the game ID 
        game_id = response.url.split('/')[-1]
        
        # Extract the date in the format '2025-04-29T09:30:00Z'
        # The time is put 3 hours after game start time
        date = response.css("time::attr(datetime)").get()

        # Team names
        # Extract both short and full team names from the span with itemprop="name"
        away_team_parts = response.xpath("(//span[@itemprop='name'])[1]//text()").getall()
        home_team_parts = response.xpath("(//span[@itemprop='name'])[2]//text()").getall()
        away_team = " ".join(part.strip() for part in away_team_parts if part.strip())
        home_team = " ".join(part.strip() for part in home_team_parts if part.strip())

        # Scores
        scores = response.css("span.score::text").getall()
        away_score, home_score = scores[0], scores[1]

        # Load batting stats
        away_batting = self.extract_batting_stats(response, "away")
        home_batting = self.extract_batting_stats(response, "home")
        
        # Load pitching stats
        away_pitching = self.extract_pitching_stats(response, "away")
        home_pitching = self.extract_pitching_stats(response, "home")

        # TeamData loaders
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

        # GameData loader
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
        rows = response.css(f"table.{team} tbody tr")
        pitchers = []
        
        for row in rows:
            stats = row.css("td.right.aligned::text").getall()
            print("Pitching stats:", stats)
            if len(stats) >= 10:
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
