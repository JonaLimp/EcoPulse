import pandas as pd
from .logger import setup_logger

class DataCleaner:
    def __init__(self):
             self.logger = setup_logger('EcoPulse')
    @staticmethod
    def clean_data(raw_data: list[dict]) -> pd.DataFrame:
        """
        Clean a list of raw data from Reddit.

        Args:
        - raw_data (list[dict]): A list of dictionaries containing raw data from Reddit.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the cleaned data.
        """
        df = pd.DataFrame(raw_data)
        df.drop_duplicates(subset="id", inplace=True)
        df.dropna(subset=["title"], inplace=True)
        df["created_datetime"] = pd.to_datetime(df["created_utc"], unit="s")
        return df
