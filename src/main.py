import os
from dotenv import load_dotenv
from .reddit_fetcher import RedditFetcher
from .database_manager import DatabaseManager
from .pipeline_manager import PipelineManager
from .data_cleaner import DataCleaner
import json

load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

with open("config.json", "r") as config_file:
    config = json.load(config_file)

DB_URL = config["database"]["url"]

SUBREDDITS = config["pipeline_settings"]["subreddits"]
KEYWORDS = config["pipeline_settings"]["keywords"]
POST_LIMIT = config["pipeline_settings"]["post_limit"]

def main():
    
    fetcher = RedditFetcher(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    cleaner = DataCleaner()
    db_manager = DatabaseManager(DB_URL)

    pipeline = PipelineManager(fetcher, cleaner, db_manager)
    pipeline.run(KEYWORDS, subreddits=SUBREDDITS, limit=POST_LIMIT)
        
if __name__ == "__main__":
    main()