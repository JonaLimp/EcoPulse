import pytest
import pandas as pd
from src.data_filter import DataFilter, RedditPostFilter, RedditCommentFilter


@pytest.fixture
def sample_dataframe():
    """Fixture to provide a sample DataFrame for testing."""
    return pd.DataFrame(
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


@pytest.fixture
def data_filter():
    """Fixture to provide a DataFilter instance."""
    return DataFilter(
        min_upvotes=10, min_comments=5, min_words=4, sentiment_threshold=0.3
    )


def test_is_engaging(data_filter, sample_dataframe):
    """Test if a post is engaging based on score."""
    assert data_filter._is_engaging(sample_dataframe.iloc[0]) is True
    assert data_filter._is_engaging(sample_dataframe.iloc[1]) is False


def test_strong_opinion(data_filter, sample_dataframe):
    """Test if a post expresses a strong opinion based on sentiment score."""
    assert data_filter._strong_opinion(sample_dataframe.iloc[0]) is True
    assert data_filter._strong_opinion(sample_dataframe.iloc[1]) is False
    assert data_filter._strong_opinion(sample_dataframe.iloc[4]) is True


@pytest.fixture
def post_filter():
    """Fixture to provide a RedditPostFilter instance."""
    return RedditPostFilter(
        min_upvotes=10, min_comments=5, min_words=5, sentiment_threshold=0.3
    )


@pytest.fixture
def sample_post_dataframe():
    """Fixture to provide sample post data for testing RedditPostFilter."""
    return pd.DataFrame(
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


def test_is_informative_post(post_filter, sample_post_dataframe):
    """Test if a Reddit post is considered informative based on title length."""
    assert post_filter._is_informative(sample_post_dataframe.iloc[0]) is True
    assert post_filter._is_informative(sample_post_dataframe.iloc[1]) is False


@pytest.fixture
def comment_filter():
    """Fixture to provide a RedditCommentFilter instance."""
    return RedditCommentFilter(
        min_upvotes=10, min_comments=5, min_words=5, sentiment_threshold=0.3
    )


@pytest.fixture
def sample_comment_dataframe():
    """Fixture to provide sample comment data for testing RedditCommentFilter."""
    return pd.DataFrame(
        [
            {
                "id": "1",
                "body": "This comment has enough words to be considered informative.",
                "score": 15,
                "sentiment_score": 0.5,
            },
            {"id": "2", "body": "Short comment.", "score": 2, "sentiment_score": 0.2},
        ]
    )


def test_is_informative_comment(comment_filter, sample_comment_dataframe):
    """Test if a Reddit comment is considered informative based on body length."""
    assert comment_filter._is_informative(sample_comment_dataframe.iloc[0]) is True
    assert comment_filter._is_informative(sample_comment_dataframe.iloc[1]) is False
