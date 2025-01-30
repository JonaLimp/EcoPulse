import unittest
from unittest.mock import patch, MagicMock
from src.reddit_fetcher import RedditFetcher 
class TestRedditFetcher(unittest.TestCase):
    """Test class for RedditFetcher"""

    @patch("src.reddit_fetcher.praw.Reddit")  
    def setUp(self, mock_praw):
        """Set up a RedditFetcher instance with a fully mocked Reddit API."""
        self.mock_reddit = mock_praw.return_value  
        self.fetcher = RedditFetcher("client_id", "client_secret", "user_agent")

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


        keywords = ["climate"]
        subreddits = ["environment"]
        limit = 100

        result = self.fetcher.fetch_reddit_posts(keywords, subreddits, limit)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["title"], "Test Post 1")
        self.assertEqual(result[1]["author"], "User2")

    def test_fetch_reddit_posts_error_handling(self):
        """Test handling of API errors."""

        self.mock_reddit.subreddit.side_effect = Exception("Reddit API error")

        keywords = ["climate"]
        subreddits = ["environment"]
        limit = 100

        result = self.fetcher.fetch_reddit_posts(keywords, subreddits, limit)

        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()
