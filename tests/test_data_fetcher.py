import pytest
from unittest.mock import MagicMock
from src.data_fetcher import RedditCommentFetcher, RedditPostFetcher


@pytest.fixture
def mock_reddit(mocker):
    """Fixture to mock the Reddit API."""
    return mocker.patch("src.data_fetcher.praw.Reddit").return_value


@pytest.fixture
def post_fetcher(mock_reddit):
    """Fixture to initialize the RedditPostFetcher with mock credentials and config."""
    return RedditPostFetcher(
        "client_id",
        "client_secret",
        "user_agent",
        {"category1": {"subreddits": ["subreddit1"], "keywords": ["keyword1"]}},
        10,
        "month",
        "sort_filter",
    )


@pytest.fixture
def comment_fetcher(mock_reddit):
    """Fixture to initialize the RedditCommentFetcher with mock credentials."""
    return RedditCommentFetcher(
        "client_id", "client_secret", "user_agent", 10, ["post_1", "post_2"]
    )


def test_fetch_reddit_posts_valid_input(mock_reddit, post_fetcher):
    """Test fetching Reddit posts with a patched API."""
    mock_subreddit = MagicMock()
    mock_reddit.subreddit.return_value = mock_subreddit

    mock_post_1 = MagicMock(id="post_1", title="Test Post 1")
    mock_post_1.author = MagicMock()
    mock_post_1.author.name = "User1"
    mock_post_2 = MagicMock(id="post_2", title="Test Post 2")
    mock_post_2.author = MagicMock()
    mock_post_2.author.name = "User2"

    mock_subreddit.search.return_value = iter([mock_post_1, mock_post_2])

    result = post_fetcher.run()

    assert len(result) == 2
    assert result[0]["title"] == "Test Post 1"
    assert result[1]["author"] == "User2"


def test_fetch_reddit_comments_valid_input(mock_reddit, comment_fetcher):
    """Test fetching Reddit comments with a patched API."""
    mock_submission = MagicMock()
    mock_reddit.submission.return_value = mock_submission

    mock_comment_1 = MagicMock(id="comment_1", body="Test Comment 1")
    mock_comment_2 = MagicMock(id="comment_2", body="Test Comment 2")

    mock_submission.comments.list.return_value = [mock_comment_1, mock_comment_2]

    result = comment_fetcher.run()

    assert len(result) == 2
    assert result[0]["body"] == "Test Comment 1"
    assert result[1]["body"] == "Test Comment 2"
