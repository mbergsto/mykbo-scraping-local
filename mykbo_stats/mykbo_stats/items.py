# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from scrapy.loader import ItemLoader
from datetime import datetime

def convert_iso_date(text):
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def strip_text(text):
    return text.strip()

def remove_hash(text):
    return text.replace('#', '')

def to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_innings_pitched(text):
    text = text.replace('\xa0', ' ').strip() # remove nbsp
    if '⅓' in text:
        base = float(text.split()[0]) if text.split()[0].isdigit() else 0
        return round(base + 1/3, 2)
    elif '⅔' in text:
        base = float(text.split()[0]) if text.split()[0].isdigit() else 0
        return round(base + 2/3, 2)
    try:
        return round(float(text), 2)
    except ValueError:
        return None



class BattingStat(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    player_name = scrapy.Field(
        output_processor=TakeFirst(),
    )
    number = scrapy.Field(
        input_processor=MapCompose(remove_hash),
        output_processor=TakeFirst()
    )
    position = scrapy.Field(
        input_processor=MapCompose(strip_text),
        output_processor=TakeFirst()
    )
    at_bats = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    runs = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    hits = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    home_runs = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    runs_batted_in = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    walks = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    strikeouts = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    
class PitchingStat(scrapy.Item):
    player_name = scrapy.Field(
        output_processor=TakeFirst()
    )
    number = scrapy.Field(
        input_processor=MapCompose(remove_hash),
        output_processor=TakeFirst()
    )
    innings_pitched = scrapy.Field(
        input_processor=MapCompose(parse_innings_pitched),
        output_processor=TakeFirst()
    )
    pitch_count = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    runs_allowed = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    earned_runs_allowed = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    hits_allowed = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    home_runs_allowed = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    strikeouts = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    walks = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    
class TeamData(scrapy.Item):
    name = scrapy.Field(
        output_processor=TakeFirst()
    )
    score = scrapy.Field(
        input_processor=MapCompose(to_int),
        output_processor=TakeFirst()
    )
    batting_stats = scrapy.Field(
    )
    pitching_stats = scrapy.Field(
    )
    
class GameData(scrapy.Item):
    game_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    date = scrapy.Field(
        input_processor=MapCompose(convert_iso_date),
        output_processor=TakeFirst()
    )
    teams = scrapy.Field() # Should be a dict with 'home' and 'away' keys mapping to TeamData objects

