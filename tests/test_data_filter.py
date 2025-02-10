import unittest
import pandas as pd
from src.data_filter import DataFilter, RedditPostFilter, RedditCommentFilter


class TestDataFilter(unittest.TestCase):
    def setUp(self):
        """
        Set up a sample DataFrame for testing.
        """
        self.df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "title": "This is a great post",
                    "body": "Very informative content about climate change.",
                    "score": 15,
                    "sentiment_score": 0.5,
                },
                {
                    "id": "2",
                    "title": "Short title",
                    "body": "Too short",
                    "score": 3,
                    "sentiment_score": -0.1,
                },
                {
                    "id": "3",
                    "title": "Engaging title",
                    "body": "This post is trending!",
                    "score": 50,
                    "sentiment_score": 0.8,
                },
                {
                    "id": "4",
                    "title": "Neutral post",
                    "body": "A very basic and neutral post.",
                    "score": 5,
                    "sentiment_score": 0.2,
                },
                {
                    "id": "5",
                    "title": "Strong opinion",
                    "body": "I strongly disagree with this!",
                    "score": 2,
                    "sentiment_score": -0.6,
                },
            ]
        )

        self.filter = DataFilter(
            min_upvotes=10, min_comments=5, min_words=4, sentiment_threshold=0.3
        )

    def test_is_engaging(self):
        """Test if a post is engaging based on score."""
        self.assertTrue(self.filter._is_engaging(self.df.iloc[0]))
        self.assertFalse(self.filter._is_engaging(self.df.iloc[1]))

    def test_strong_opinion(self):
        """Test if a post expresses a strong opinion based on sentiment score."""
        self.assertTrue(self.filter._strong_opinion(self.df.iloc[0]))
        self.assertFalse(self.filter._strong_opinion(self.df.iloc[1]))
        self.assertTrue(self.filter._strong_opinion(self.df.iloc[4]))


class TestRedditPostFilter(unittest.TestCase):
    def setUp(self):
        """
        Set up sample post data for testing RedditPostFilter.
        """
        self.df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "title": "An informative long title for Reddit",
                    "score": 20,
                    "sentiment_score": 0.4,
                },
                {"id": "2", "title": "Short title", "score": 5, "sentiment_score": 0.1},
            ]
        )
        self.post_filter = RedditPostFilter(
            min_upvotes=10, min_comments=5, min_words=5, sentiment_threshold=0.3
        )

    def test_is_informative_post(self):
        """Test if a Reddit post is considered informative based on title length."""
        self.assertTrue(self.post_filter._is_informative(self.df.iloc[0]))
        self.assertFalse(self.post_filter._is_informative(self.df.iloc[1]))


class TestRedditCommentFilter(unittest.TestCase):
    def setUp(self):
        """
        Set up sample comment data for testing RedditCommentFilter.
        """
        self.df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "body": """This comment has enough words to
             be considered informative.""",
                    "score": 15,
                    "sentiment_score": 0.5,
                },
                {
                    "id": "2",
                    "body": "Short comment.",
                    "score": 2,
                    "sentiment_score": 0.2,
                },
            ]
        )
        self.comment_filter = RedditCommentFilter(
            min_upvotes=10, min_comments=5, min_words=5, sentiment_threshold=0.3
        )

    def test_is_informative_comment(self):
        """Test if a Reddit comment is considered
        informative based on body length."""
        self.assertTrue(self.comment_filter._is_informative(self.df.iloc[0]))
        self.assertFalse(self.comment_filter._is_informative(self.df.iloc[1]))


if __name__ == "__main__":
    unittest.main()
