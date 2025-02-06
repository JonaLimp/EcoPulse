import praw
from .data_processor import DataProcessor
from typing import Any


class RedditDataFetcher(DataProcessor):
    def __init__(
        self, client_id: str, client_secret: str, user_agent: str, limit: int
    ) -> None:
        """
        Initializes the Fetcher object.

        Parameters:
        - client_id (str): Reddit API client ID
        - client_secret (str): Reddit API client secret
        - user_agent (str): Reddit API user agent string

        Returns:
        - None
        """
        super().__init__()

        self._reddit = praw.Reddit(
            client_id=client_id, client_secret=client_secret, user_agent=user_agent
        )

        self._limit = limit


class RedditCommentFetcher(RedditDataFetcher):
    def __init__(
        self, client_id, client_secret, user_agent, limit, post_ids: list[str]
    ):
        super().__init__(client_id, client_secret, user_agent, limit)
        self._post_ids = post_ids

    def run(self) -> list[dict[str]]:
        """
        Fetches comments for a list of Reddit posts.

        Args:
            post_ids (list[str]): List of post IDs.
            limit (int): Number of comments to fetch per post.

        Returns:
            list[dict[str, any]]: List of dictionaries containing comment data.
        """
        comments = []
        for post_id in self._post_ids:
            post = self._reddit.submission(id=post_id)
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                comments.append(
                    {
                        "id": comment.id,
                        "post_id": post_id,
                        "author": comment.author.name if comment.author else None,
                        "subreddit": post.subreddit.display_name,
                        "body": comment.body,
                        "score": comment.score,
                        "created_utc": comment.created_utc,
                    }
                )
        return comments


class RedditPostFetcher(RedditDataFetcher):
    def __init__(
        self, client_id: str, client_secret: str, user_agent: str, categories, limit
    ):
        super().__init__(client_id, client_secret, user_agent, limit)
        self._categories = categories

    def run(self) -> Any:
        """
        Fetches Reddit posts based on keywords.

        Args:
            categories (list[dict[str, list[str]]]): List of categories,
            each containing subreddits and keywords.
            limit (int): Number of posts to fetch per keyword.

        Returns:
            list[dict[str, any]]: List of dictionaries containing post data.
        """
        posts = []
        for category in self._categories:
            subreddits = self._categories[category]["subreddits"]
            for subreddit in subreddits:
                self.logger.info(f"Searching in subreddit: {subreddit}")
                keywords = self._categories[category]["keywords"]
                for keyword in keywords:
                    self.logger.info(f"Searching for keyword: {keyword}")
                    try:
                        for submission in self._reddit.subreddit(subreddit).search(
                            keyword, sort="new", limit=self._limit
                        ):
                            posts.append(
                                {
                                    "id": submission.id,
                                    "title": submission.title,
                                    "author": (
                                        submission.author.name
                                        if submission.author
                                        else None
                                    ),
                                    "subreddit": submission.subreddit.display_name,
                                    "content": submission.selftext,
                                    "created_utc": submission.created_utc,
                                    "score": submission.score,
                                    "url": submission.url,
                                    "num_comments": submission.num_comments,
                                    "category": category,
                                    "keyword": keyword,
                                }
                            )
                    except Exception as e:
                        self.logger.error(f"Error fetching posts: {e}")

        return posts
