import os
import json

from dotenv import load_dotenv

from .data_fetcher import RedditPostFetcher, RedditCommentFetcher
from .database_manager import CommentDataBaseManager, PostDataBaseManager
from .pipeline_manager import PipelineManager
from .data_cleaner import RedditPostCleaner, RedditCommentCleaner
from .data_enricher import RedditCommentEnricher, RedditPostEnricher
from .data_filter import RedditCommentFilter, RedditPostFilter

load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")

with open("_config.json") as config_file:
    config = json.load(config_file)

DB_URL = config["database"]["url"]
DB_URL = f"""mysql+mysqlconnector://
{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}/{DATABASE_NAME}"""

CATEGORIES = config["categories"]
POST_LIMIT = config["fetcher_settings"]["post_limit"]
MORE_COMMENTS_LIMIT = config["fetcher_settings"]["more_comments_limit"]
TIME_FILTER = config["fetcher_settings"]["time_filter"]
SORT_FILTER = config["fetcher_settings"]["sort_filter"]

MIN_UPVOTES = config["filter_settings"]["min_upvotes"]
MIN_COMMENTS = config["filter_settings"]["min_comments"]
MIN_WORDS = config["filter_settings"]["min_words"]
SENTIMENT_THRESHOLD = config["filter_settings"]["sentiment_threshold"]


def main():
    post_fetcher = RedditPostFetcher(
        REDDIT_CLIENT_ID,
        REDDIT_CLIENT_SECRET,
        REDDIT_USER_AGENT,
        CATEGORIES,
        POST_LIMIT,
        TIME_FILTER,
        SORT_FILTER,
    )

    post_cleaner = RedditPostCleaner()
    post_enricher = RedditPostEnricher()
    post_filter = RedditPostFilter(
        min_upvotes=MIN_UPVOTES,
        min_comments=MIN_COMMENTS,
        min_words=MIN_WORDS,
        sentiment_threshold=SENTIMENT_THRESHOLD,
    )
    post_db_manager = PostDataBaseManager(
        DB_URL, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME
    )

    post_pipeline = PipelineManager(
        fetcher=post_fetcher,
        cleaner=post_cleaner,
        enricher=post_enricher,
        filter=post_filter,
        db_manager=post_db_manager,
    )
    post_filtered_data = post_pipeline.run()

    comment_fetcher = RedditCommentFetcher(
        REDDIT_CLIENT_ID,
        REDDIT_CLIENT_SECRET,
        REDDIT_USER_AGENT,
        MORE_COMMENTS_LIMIT,
        post_filtered_data["id"],
    )
    comment_cleaner = RedditCommentCleaner()
    comment_enricher = RedditCommentEnricher()
    comment_filter = RedditCommentFilter()
    comment_db_mananger = CommentDataBaseManager(
        DB_URL, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME
    )
    comment_pipeline = PipelineManager(
        fetcher=comment_fetcher,
        cleaner=comment_cleaner,
        enricher=comment_enricher,
        filter=comment_filter,
        db_manager=comment_db_mananger,
    )
    comment_pipeline.run()


if __name__ == "__main__":
    main()
