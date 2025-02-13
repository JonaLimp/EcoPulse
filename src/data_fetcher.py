import praw
from .data_processor import DataProcessor
from typing import Any


class RedditDataFetcher(DataProcessor):
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
        super().__init__()

        self._reddit = praw.Reddit(
            client_id=client_id, client_secret=client_secret, user_agent=user_agent
        )

    @staticmethod
    def _print_progress(index: int, previous_progress: int, length_data: int):
        """
        Prints the progress of fetching data as a percentage.

        Args:
        - index (int): Current index of the data being fetched.
        - previous_progress (int): The previous progress (in 10% increments).
        - length_data (int): The total length of the data being fetched.

        Returns:
        - int: The new previous progress (in 10% increments).
        """
        progress = int((index + 1) / length_data * 100)
        if progress // 10 > previous_progress // 10:
            previous_progress = progress
            print(f"Progress: {progress}% ({index + 1}/{length_data})")

        return previous_progress


class RedditCommentFetcher(RedditDataFetcher):
    def __init__(
        self,
        client_id,
        client_secret,
        user_agent,
        more_comments_limit,
        post_ids: list[str],
    ):
        """
        Initializes the CommentFetcher object.

        Parameters:
        - client_id (str): Reddit API client ID
        - client_secret (str): Reddit API client secret
        - user_agent (str): Reddit API user agent string
        - more_comments_limit (int): The limit for fetching more comments.
        - post_ids (list[str]): List of post IDs.

        Returns:
        - None
        """
        super().__init__(client_id, client_secret, user_agent)
        self._post_ids = post_ids
        self._more_comments_limit = more_comments_limit

    def run(self) -> list[dict[str, Any]]:
        """
        Fetches comments for a list of Reddit posts.

        Args:
            post_ids (list[str]): List of post IDs.
            limit (int): Number of comments to fetch per post.

        Returns:
            list[dict[str, Any]]: List of dictionaries containing comment data.
        """
        self._logger.info("Fetching Comment Data...")
        comments = []
        previous_progress = -1

        for index, post_id in enumerate(self._post_ids):
            previous_progress = self._print_progress(
                index, previous_progress, len(self._post_ids)
            )

            post = self._reddit.submission(id=post_id)
            post.comments.replace_more(limit=self._more_comments_limit)
            for comment in post.comments.list():
                comments.append(
                    {
                        "id": comment.id,
                        "post_id": post_id,
                        "author": getattr(comment.author, "name", None),
                        "subreddit": post.subreddit.display_name,
                        "body": comment.body,
                        "score": comment.score,
                        "created_utc": comment.created_utc,
                    }
                )

        self._logger.info(f"Fetched Data: #{len(comments)}")
        return comments


class RedditPostFetcher(RedditDataFetcher):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str,
        categories: dict[str, list[str]],
        post_limit: int,
        time_filter: str,
        sort_filter: str,
    ):
        super().__init__(client_id, client_secret, user_agent)
        self._categories = categories
        self._post_limit = post_limit
        self._time_filter = time_filter
        self._sort_filter = sort_filter

    def run(self) -> list[dict[str, Any]]:
        """
        Fetches Reddit posts based on keywords.

        Args:
            categories (list[dict[str, list[str]]]): List of categories,
            each containing subreddits and keywords.
            limit (int): Number of posts to fetch per keyword.

        Returns:
            list[dict[str, any]]: List of dictionaries containing post data.
        """
        self._logger.info("Fetching Post data...")
        posts = []
        previous_progress = -1

        for index, category in enumerate(self._categories):
            subreddits = self._categories[category]["subreddits"]
            for subreddit in subreddits:
                keywords = self._categories[category]["keywords"]
                self._logger.info(f"Searching in {subreddit} for keywords: {keywords}")

                for keyword in keywords:
                    try:
                        for submission in self._reddit.subreddit(subreddit).search(
                            keyword,
                            sort=self._sort_filter,
                            time_filter=self._time_filter,
                            limit=self._post_limit,
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
                        self._logger.error(
                            f"Error fetching {keyword} in subreddtit: {subreddit}: {e}"
                        )

            previous_progress = self._print_progress(
                index, previous_progress, len(self._categories)
            )

        self._logger.info(f"Fetched Data: #{len(posts)}")
        return posts
