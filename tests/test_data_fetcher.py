import unittest
from unittest.mock import patch, MagicMock

from src.reddit_fetcher import RedditCommentFetcher, RedditPostFetcher


class TestRedditPostFetcher(unittest.TestCase):
    """Test class for RedditFetcher"""

    @patch("src.reddit_fetcher.praw.Reddit")
    def setUp(self, mock_praw):
        """Set up a RedditFetcher instance with a fully mocked Reddit API."""
        self.mock_reddit = mock_praw.return_value
        self.fetcher = RedditPostFetcher(
            "client_id",
            "client_secret",
            "user_agent",
            {"category1": {"subreddits": ["subreddit1"], "keywords": ["keyword1"]}},
            10,
        )

    def test_fetch_reddit_posts_valid_input(self):
        """Test fetching Reddit posts with a patched API."""

        mock_subreddit = MagicMock()
        self.mock_reddit.subreddit.return_value = mock_subreddit

        mock_post_1 = MagicMock(id="post_1", title="Test Post 1")
        mock_post_1.author = MagicMock()
        mock_post_1.author.name = "User1"
        mock_post_2 = MagicMock(id="post_2", title="Test Post 2")
        mock_post_2.author = MagicMock()
        mock_post_2.author.name = "User2"

        mock_subreddit.search.return_value = iter([mock_post_1, mock_post_2])

        result = self.fetcher.run()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["title"], "Test Post 1")
        self.assertEqual(result[1]["author"], "User2")


class TestRedditCommentFetcher(unittest.TestCase):
    """Test class for RedditFetcher"""

    @patch("src.reddit_fetcher.praw.Reddit")
    def setUp(self, mock_praw):
        """Set up a RedditFetcher instance with a fully mocked Reddit API."""
        self.mock_reddit = mock_praw.return_value
        self.fetcher = RedditCommentFetcher(
            "client_id", "client_secret", "user_agent", 10, ["post_1", "post_2"]
        )

    def test_fetch_reddit_posts_valid_input(self):
        """Test fetching Reddit posts with a patched API."""

        mock_subreddit = MagicMock()
        self.mock_reddit.submission.return_value = mock_subreddit

        mock_post_1 = MagicMock(id="post_1", title="Test Comment 1")
        mock_post_1.body = "Test Comment 1"
        mock_post_2 = MagicMock(id="post_2", title="Test Comment 2")
        mock_post_2.author = MagicMock()
        mock_post_2.body = "Test Comment 2"

        mock_subreddit.comments.list.return_value = [mock_post_1, mock_post_2]

        result = self.fetcher.run()

        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["body"], "Test Comment 1")
        self.assertEqual(result[1]["body"], "Test Comment 2")


if __name__ == "__main__":
    unittest.main()
