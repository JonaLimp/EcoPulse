import pandas as pd
import re
from .data_processor import DataProcessor


class RedditCleaner(DataProcessor):
    """Base class for cleaning Reddit data (posts & comments)."""

    def __init__(self):
        super().__init__()

    @staticmethod
    def _clean_text(text: str):
        """
        Cleans text by:
        - Removing URLs, @mentions, and hashtags.
        - Keeping only alphanumeric characters and basic punctuation.
        - Removing extra spaces and converting to lowercase.
        """
        if not isinstance(text, str) or text.strip() == "":
            return "Content unavailable"

        text = text.lower().strip()
        text = re.sub(r"http\S+|www\S+", "", text)  # Remove URLs
        text = re.sub(r"@\w+", "", text)  # Remove @mentions
        text = re.sub(r"#\w+", "", text)  # Remove hashtags
        text = re.sub(r"[^a-zA-Z0-9\s.,!?]", "", text)  # Remove special characters
        text = re.sub(r"\s+", " ", text).strip()  # Remove extra spaces

        return text

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans common fields across all Reddit data.

        Args:
        - data (pd.DataFrame): Raw Reddit data.

        Returns:
        - pd.DataFrame: Cleaned data.
        """
        self._logger.info("Cleaning data...")

        data = data.copy()
        data = data.fillna(
            {
                "author": "Anonymous",
                "sentiment_score": 0,
                "score": 0,
                "created_utc": int(pd.Timestamp.now().timestamp()),
            }
        )

        data.loc[:, "created_datetime"] = pd.to_datetime(data["created_utc"], unit="s")

        return data


class RedditPostCleaner(RedditCleaner):
    """Cleans Reddit post data before database insertion."""

    def __init__(self):
        super().__init__()

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans Reddit posts by:
        - Removing missing/deleted titles.
        - Cleaning title text.

        Args:
        - data (pd.DataFrame): Raw Reddit post data.

        Returns:
        - pd.DataFrame: Cleaned post data.
        """
        data = super().run(data)

        data = data[data["title"].notna()].copy()
        data = data[~data["title"].isin(["[deleted]", "[removed]"])].copy()

        data.loc[:, "title"] = data["title"].apply(self._clean_text)
        data.loc[:, "title"] = data["title"].fillna("Content unavailable")

        self._logger.info(f"Remaining records: {data.shape[0]}")
        return data


class RedditCommentCleaner(RedditCleaner):
    """Cleans Reddit comment data before database insertion."""

    def __init__(self):
        super().__init__()

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans Reddit comments by:
        - Removing missing/deleted comments.
        - Cleaning comment text.
        - Ensuring MySQL-safe data types.

        Args:
        - data (pd.DataFrame): Raw Reddit comment data.

        Returns:
        - pd.DataFrame: Cleaned comment data.
        """
        data = super().run(data)

        data = data[data["body"].notna()].copy()
        data = data[~data["body"].isin(["[deleted]", "[removed]"])].copy()

        data.loc[:, "body"] = data["body"].apply(self._clean_text)
        data.loc[:, "body"] = data["body"].fillna("Content unavailable")

        self._logger.info(f"Remaining records: {data.shape[0]}")
        return data
