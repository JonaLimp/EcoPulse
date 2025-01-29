from sqlalchemy import create_engine
from .logger import setup_logger

class DatabaseManager:
    def __init__(self, db_url):
        self.logger = setup_logger()
        self.engine = create_engine(db_url)

    def store_data(self, df, table_name="posts"):
        df.to_sql(table_name, self.engine, if_exists="append", index=False)
        self.logger.info(f"{len(df)} records inserted into {table_name}.")
