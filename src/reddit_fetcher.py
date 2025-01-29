from datetime import datetime
import praw
from .logger import setup_logger

class RedditFetcher:
    def __init__(self, client_id: str, client_secret: str, user_agent: str) -> None:
        """
        Initializes the Fetcher object.

        Parameters:
        - client_id (str): Reddit API client ID
        - client_secret (str): Reddit API client secret
        - user_agent (str): Reddit API user agent string

        Returns:
        - None
        """
        self.logger = setup_logger()
        self.reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
)
    

    def fetch_reddit_posts(self, keywords: list[str], subreddits: list[str] = "all", limit: int = 100) -> list[dict]:
        """
        Fetches Reddit posts based on keywords.

        Parameters:
        - keywords (list[str]): List of keywords to search for.
        - subreddit (str): Subreddit to search in ("all" for all subreddits).
        - limit (int): Number of posts to fetch per keyword.

        Returns:
        - list[dict]: List of dictionaries containing post data.
        """
        posts = []
        for subreddit in subreddits:
            for keyword in keywords:
                self.logger.info(f"Searching for keyword: {keyword}")
                try:
                    for submission in self.reddit.subreddit(subreddit).search(keyword, sort='new', limit=limit):
                        posts.append({
                            "id": submission.id,
                            "title": submission.title,
                            "author": submission.author.name if submission.author else None,
                            "subreddit": submission.subreddit.display_name,
                            "created_utc": submission.created_utc,
                            "score": submission.score,
                            "url": submission.url,
                            "num_comments": submission.num_comments,
                            "created_datetime": self.format_datetime(submission.created_utc),
                        })
                except Exception as e:
                    self.logger.error(f"Error fetching posts: {e}") 

        return posts


    def fetch_reddit_comments(self, post_ids: list[str], limit: int = 100) -> list[dict]:
        """
        Fetches comments for a list of Reddit posts.

        Parameters:
        - post_ids (list[str]): List of post IDs.
        - limit (int): Number of comments to fetch per post.

        Returns:
        - list[dict]: List of dictionaries containing comment data.
        """
        comments = []
        for post_id in post_ids:
            submission: praw.models.Submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)  
            for comment in submission.comments.list():
                comments.append({
                    "id": comment.id,
                    "post_id": post_id,
                    "author": comment.author.name if comment.author else None,
                    "subreddit": submission.subreddit.display_name,
                    "body": comment.body,
                    "score": comment.score,
                    "created_utc": comment.created_utc,
                    "created_datetime": self.format_datetime(submission.created_utc),
                })
        return comments

    def format_datetime(self, utc_timestamp: int) -> str:
        """
        Converts a Unix timestamp (int) to a human-readable datetime string (str).

        Parameters:
        - utc_timestamp (int): Unix timestamp

        Returns:
        - str: Human-readable datetime string in the format %Y-%m-%d %H:%M:%S
        """
        return datetime.utcfromtimestamp(utc_timestamp).strftime('%Y-%m-%d %H:%M:%S')
