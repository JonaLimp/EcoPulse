from pandas import DataFrame
from .data_processor import DataProcessor


class DataFilter(DataProcessor):
    def __init__(
        self,
        min_upvotes: int = 10,
        min_comments: int = 5,
        min_words: int = 20,
        sentiment_threshold: float = 0.3,
    ) -> None:
        super().__init__()
        """
        Initialize filter parameters.

        :param min_upvotes: int - Minimum number of upvotes for a
        post/comment to be considered valuable.
        :param min_comments: int - Minimum number of comments to
        qualify as engaging.
        :param min_words: int - Minimum word count to filter out short,
        low-value content.
        :param sentiment_threshold: float - Threshold for strong sentiment
        (positive or negative).
        """
        self.min_upvotes = min_upvotes
        self.min_comments = min_comments
        self.min_words = min_words
        self.sentiment_threshold = sentiment_threshold

    def _is_valuable(self, data: DataFrame) -> bool:
        """
        Determines if a Reddit comment is valuable.

        :param comment: A dictionary containing comment data.
        :return: True if the comment meets filtering criteria, False otherwise.
        """
        return (
            self._is_engaging(data)
            or self._is_informative(data)
            or self._strong_opinion(data)
        )

    def _is_engaging(self, data: DataFrame) -> bool:
        """
        Determines if a Reddit post or comment is engaging based on upvotes.

        :param data: DataFrame containing Reddit post or comment data.
        :return: True if the score of the post or comment meets or exceeds
        the minimum upvotes threshold, False otherwise.
        """

        return data["score"] >= self.min_upvotes

    def _is_informative(self, data: DataFrame) -> bool:
        pass

    def _strong_opinion(self, data: DataFrame) -> bool:
        return (
            data["sentiment_score"] >= self.sentiment_threshold
            or data["sentiment_score"] <= -1 * self.sentiment_threshold
        )

    def run(self, df: DataFrame) -> DataFrame:
        df = df[df.apply(lambda row: self._is_valuable(row), axis=1)]
        return df


class RedditPostFilter(DataFilter):
    def _is_informative(self, data: DataFrame) -> bool:
        return len(data["title"].split()) >= self.min_words


class RedditCommentFilter(DataFilter):
    def _is_informative(self, data: DataFrame) -> bool:
        return len(data["body"].split()) >= self.min_words
