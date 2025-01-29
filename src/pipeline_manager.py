from .logger import setup_logger
class PipelineManager:
    def __init__(self, fetcher, cleaner, db_manager):
        """
        Initializes a PipelineManager object with the given fetcher, cleaner and db_manager instances.

        Parameters:
        - fetcher (BaseFetcher): The fetcher object that will be used to fetch data from the source.
        - cleaner (BaseDataCleaner): The cleaner object that will be used to clean the fetched data.
        - db_manager (BaseDBManager): The db_manager object that will be used to store the cleaned data.
        """
        self.logger = setup_logger()
        self.fetcher = fetcher
        self.cleaner = cleaner
        self.db_manager = db_manager

    def run(self, keywords, subreddit="all", limit=100):
        self.logger.info("Fetching data...")
        raw_data = self.fetcher.fetch_reddit_posts(keywords, subreddit, limit)

        self.logger.info("Cleaning data...")
        cleaned_data = self.cleaner.clean_data(raw_data)

        self.logger.info("Storing data...")
        try:
            self.db_manager.store_data(cleaned_data)
        except Exception as e:
            self.logger.error(f"Error occurred while storing data: {str(e)}")

        self.logger.info("Pipeline executed successfully!")
