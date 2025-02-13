from pandas import DataFrame
from .logger import setup_logger
from .data_cleaner import RedditCleaner
from .data_enricher import DataEnricher
from .data_filter import DataFilter
from .data_fetcher import RedditDataFetcher
from .database_manager import DatabaseManager


class PipelineManager:
    def __init__(
        self,
        fetcher: RedditDataFetcher,
        cleaner: RedditCleaner,
        enricher: DataEnricher,
        filter: DataFilter,
        db_manager: DatabaseManager,
    ) -> None:
        """
        Initializes a PipelineManager object with the given fetcher,
        cleaner and db_manager instances.

        Parameters:
        - fetcher (BaseFetcher): The fetcher object that will be used
        to fetch data from the source.
        - cleaner (BaseDataCleaner): The cleaner object that will be used
        to clean the fetched data.
        - db_manager (BaseDBManager): The db_manager object that will be used
        to store the cleaned data.
        """
        self._logger = setup_logger("EcoPulse")
        self._fetcher = fetcher
        self._enricher = enricher
        self._filter = filter
        self._cleaner = cleaner
        self._db_manager = db_manager

    def run(self) -> DataFrame:
        """
        Runs the pipeline to fetch, clean and store data from Reddit.

        The pipeline fetches data from Reddit, cleans it and stores
        it in the database.

        Args:
            keywords (list[str]): List of keywords to search for.
            subreddits (str): Subreddit to search in ("all" for all subreddits).
            limit (int): Number of posts to fetch per keyword.

        Returns:
            None
        """
        raw_data = DataFrame(self._fetcher.run())
        cleaned_data = self._cleaner.run(raw_data)
        enriched_data = self._enricher.run(cleaned_data)
        filtered_data = self._filter.run(enriched_data)

        try:
            self._db_manager.run(cleaned_data)
        except Exception as e:
            self._logger.error(
                f"""Error occurred while
                               storing post data: {str(e)}"""
            )

        self._logger.info("Pipeline executed!")
        return filtered_data
