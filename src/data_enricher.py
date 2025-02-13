from pandas import DataFrame
from textblob import TextBlob
from .data_processor import DataProcessor
from datetime import datetime


class DataEnricher(DataProcessor):
    def __init__(self):
        super().__init__()

    def _format_datetime(self, utc_timestamp: int) -> str:
        """
        Converts a Unix timestamp (int) to a human-readable datetime
        string (str).

        Parameters:
        - utc_timestamp (int): Unix timestamp

        Returns:
        - str: Human-readable datetime string in the format %Y-%m-%d %H:%M:%S
        """
        return datetime.utcfromtimestamp(utc_timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def _get_sentiment_score(self, text: str) -> float | None:
        """
        Calculates the sentiment score for a given text string.

        Returns a float from -1.0 (very negative sentiment) to 1.0
        (very positive sentiment),
        or None if the input is empty or None.

        Parameters:
        - text (str): Text string to analyze.

        Returns:
        - float | None: Sentiment score, or None if input is invalid.
        """
        if not text or text == "":
            return None
        return TextBlob(text).sentiment.polarity


class RedditCommentEnricher(DataEnricher):
    def __init__(self):
        super().__init__()

    def run(self, data: DataFrame) -> DataFrame:
        """
        Enriches a Pandas DataFrame containing Reddit comment data.

        Adds two new columns to the DataFrame:
        - `created_datetime`: A human-readable datetime string in the
        format %Y-%m-%d %H:%M:%S
        - `sentiment_score`: A float from -1.0 (very negative sentiment) to
        1.0 (very positive sentiment)

        Parameters:
        - data (pd.DataFrame): A Pandas DataFrame containing Reddit post data.

        Returns:
        - pd.DataFrame: A Pandas DataFrame with the enriched columns.
        """
        self._logger.info("Enrich data...")
        data["created_datetime"] = data["created_utc"].apply(self._format_datetime)

        data["sentiment_score"] = data["body"].apply(self._get_sentiment_score)
        data["sentiment_score"] = data["sentiment_score"].fillna(0).astype(float)

        return data


class RedditPostEnricher(DataEnricher):
    def __init__(self):
        super().__init__()

    def run(self, data: DataFrame) -> DataFrame:
        """
        Enriches a Pandas DataFrame containing Reddit post data.

        Adds two new columns to the DataFrame:
        - `created_datetime`: A human-readable datetime string in the
        format %Y-%m-%d %H:%M:%S
        - `sentiment_score`: A float from -1.0 (very negative sentiment)
        to 1.0 (very positive sentiment)

        Parameters:
        - data (pd.DataFrame): A Pandas DataFrame containing Reddit post data.

        Returns:
        - pd.DataFrame: A Pandas DataFrame with the enriched columns.
        """
        data["created_datetime"] = data["created_utc"].apply(self._format_datetime)
        data["sentiment_score"] = data["title"].apply(self._get_sentiment_score)
        return data
