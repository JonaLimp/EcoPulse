from .data_processor import DataProcessor

class DataFilter(DataProcessor):
    
    def __init__(self, min_upvotes=10, min_comments=5, min_words=20, sentiment_threshold=0.3):
        super().__init__()
        """
        Initialize filter parameters.

        :param min_upvotes: Minimum number of upvotes for a post/comment to be considered valuable.
        :param min_comments: Minimum number of comments to qualify as engaging.
        :param min_words: Minimum word count to filter out short, low-value content.
        :param min_sentiment: Threshold for strong sentiment (positive or negative).
        :param trusted_sources: List of trusted sources for link-based credibility.
        """
        self.min_upvotes = min_upvotes
        self.min_comments = min_comments
        self.min_words = min_words
        self.sentiment_threshold = sentiment_threshold

    def _is_valuable(self, data):
        """
        Determines if a Reddit comment is valuable.

        :param comment: A dictionary containing comment data.
        :return: True if the comment meets filtering criteria, False otherwise.
        """
        return self._is_engaging(data) or self._is_informative(data) or self._strong_opinion(data)
    
    def _is_engaging(self, data):
        return data["score"] >= self.min_upvotes
    
    def _is_informative(self, data):
        pass
    
    def _strong_opinion(self, data):
         data['sentiment_score'] >= self.sentiment_threshold or data['sentiment_score'] <= -1 *self.sentiment_threshold
         
    def run(self, df: 'Dataframe') -> 'Dataframe':
        df = df[df.apply(lambda row: self._is_valuable(row), axis=1)]
        return df 
    

class RedditPostFilter(DataFilter):
    
    def _is_informative(self, data):
        return len(data["title"].split()) >= self.min_words
        
class RedditCommentFilter(DataFilter):
    
    def _is_informative(self, data):
        return len(data["body"].split()) >= self.min_words