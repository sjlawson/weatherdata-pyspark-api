import logging
from abc import ABC
from flask import current_app
from datetime import datetime
from app.spark_helpers import get_spark, df_to_db

log = logging.getLogger(__name__)


class BaseETL(ABC):
    """
    extend this class to a child etl process and run with child.go_etl()
    """

    loader: object

    def __init__(self, collector, formatter, db_model):
        self.collector = collector  # -> gets list of files/tasks
        self.formatter = formatter  # -> handler for data transformations
        self.schema = db_model.spark_schema()
        self.db_table = db_model.__tablename__
        self.db_model = db_model

    def bulk_read_csv(self, paths: list):
        """
        Spark can read in bulk if they are formatted the same
        though it can simply take a directory, it's a little easier to maintain
        and debug if we feed it a list of specific files, and it's still really fast.
        """
        log.debug(f"Reading CSV files: {paths}")
        df = current_app.spark.read.options(delimiter="\t").csv(paths, self.schema)
        return df

    def go_etl(self):
        log.info(f"Starting ETL process for {self.db_table}")
        log.info(f"Ingestor start time: {datetime.utcnow()} UTC")
        paths = self.collector()
        df = self.bulk_read_csv(paths)
        df = self.formatter(df)
        load_count = self.loader(df)
        log.info(f"{self.db_table} ingestor end time: {datetime.utcnow()} UTC")
        log.info(f"Total records loaded: {load_count}")

    def spark_db_write(self, df):
        """
        Of all the ways you can write a dataframe to a table, this is definitely one of them.
        Since the df is passed to the loader method, it could be done there, but I feel like
        it's a bit cleaner to keep the spark jdbc stuff as contained as possible.
        """
        before_count = self.db_model.query.count()
        df_count = df.count()
        df_to_db(df, self.db_table)
        after_count = self.db_model.query.count()
        load_count = after_count - before_count
        return load_count if load_count <= df_count else df_count
