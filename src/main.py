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

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")

with open("config.json", "r") as config_file:
    config = json.load(config_file)

DB_URL = config["database"]["url"]
DB_URL = f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}/{DATABASE_NAME}"

CATEGORIES = config["pipeline_settings"]["categories"]
POST_LIMIT = config["pipeline_settings"]["post_limit"]

def main():
    
    fetcher = RedditFetcher(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    cleaner = DataCleaner()
    db_manager = DatabaseManager(DB_URL, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME)

    pipeline = PipelineManager(fetcher, cleaner, db_manager)
    pipeline.run(categories=CATEGORIES, limit=POST_LIMIT,
                 run_comments=False
                 )
        
if __name__ == "__main__":
    main()