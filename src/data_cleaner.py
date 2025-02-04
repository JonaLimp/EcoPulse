import pandas as pd
import re
from .data_processor import DataProcessor


class RedditCleaner(DataProcessor):
    
    @staticmethod
    def _clean_text(text):
        """Cleans Reddit comment text by removing unwanted elements."""
        if not text or text.strip() == "":  
            return None

        text = text.lower()  # Convert to lowercase
        text = re.sub(r"http\S+|www\S+", "", text)  # Remove URLs
        text = re.sub(r"@\w+", "", text)  # Remove @mentions
        text = re.sub(r"#\w+", "", text)  # Remove hashtags
        text = re.sub(r"[^a-zA-Z0-9\s.,!?]", "", text)  # Remove special characters (except punctuation)
        text = re.sub(r"\s+", " ", text).strip()  # Remove extra spaces and newlines

        return text
            

class RedditPostCleaner(RedditCleaner):
    
    def __init__(self):
        super().__init__()
        
    def run(self, data: list[dict]) -> pd.DataFrame:
        """
        Clean a list of raw data from Reddit.

        Args:
        - raw_data (list[dict]): A list of dictionaries containing raw data from Reddit.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the cleaned data.
        """
        df = pd.DataFrame(data)
        df["title"] = df["title"].apply(self._clean_text)
        df.drop_duplicates(subset="id", inplace=True)
        df.dropna(subset=["title"], inplace=True)
        df["created_datetime"] = pd.to_datetime(df["created_utc"], unit="s")
        return df

class RedditCommentCleaner(RedditCleaner):
    
    def __init__(self):
        super().__init__()
        
    def run(self, data: list[dict]) -> pd.DataFrame:
        """
        Clean a list of raw data from Reddit.

        Args:
        - raw_data (list[dict]): A list of dictionaries containing raw data from Reddit.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the cleaned data.
        """
        df = pd.DataFrame(data)
        df = df[df["body"].notna()]
        df = df[~df["body"].isin(["[deleted]", "[removed]"])]
        df["body"] = df["body"].apply(self._clean_text)
        return df