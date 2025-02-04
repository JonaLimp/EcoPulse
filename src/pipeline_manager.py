from .logger import setup_logger
class PipelineManager:
    def __init__(self, fetcher: 'BaseFetcher', cleaner: 'BaseDataCleaner', db_manager: 'BaseDBManager') -> None:
        """
        Initializes a PipelineManager object with the given fetcher, cleaner and db_manager instances.

        Parameters:
        - fetcher (BaseFetcher): The fetcher object that will be used to fetch data from the source.
        - cleaner (BaseDataCleaner): The cleaner object that will be used to clean the fetched data.
        - db_manager (BaseDBManager): The db_manager object that will be used to store the cleaned data.
        """
        self.logger = setup_logger("EcoPulse")
        self.fetcher = fetcher
        self.cleaner = cleaner
        self.db_manager = db_manager

    def run(self, categories: list[dict[str]], limit: int = 100, run_posts: bool = True, run_comments: bool = True) -> None:
        """
        Runs the pipeline to fetch, clean and store data from Reddit.

        The pipeline fetches data from Reddit, cleans it and stores it in the database.

        Args:
            keywords (list[str]): List of keywords to search for.
            subreddits (str): Subreddit to search in ("all" for all subreddits).
            limit (int): Number of posts to fetch per keyword.

        Returns:
            None
        """
        if run_posts == True:
            self.logger.info("Fetching post data...")
            raw_data = self.fetcher.fetch_reddit_posts(categories, limit)

            self.logger.info("Cleaning post data...")
            cleaned_data = self.cleaner.clean_data(raw_data)

            self.logger.info("Storing post data...")
            try:
                self.db_manager.store_posts(cleaned_data)
            except Exception as e:
                self.logger.error(f"Error occurred while storing post data: {str(e)}")

        if run_comments == True:
            self.logger.info("Fetching comment data...")
            raw_data = self.fetcher.fetch_reddit_comments(categories, limit)

            self.logger.info("Cleaning comment data...")
            cleaned_data = self.cleaner.clean_data(raw_data)

            self.logger.info("Storing comment data...")
            try:
                self.db_manager.store_comments(cleaned_data)
            except Exception as e:
                self.logger.error(f"Error occurred while storing comment data: {str(e)}")

        self.logger.info("Pipeline executed successfully!")

