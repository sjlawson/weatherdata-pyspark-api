import logging

from flask import current_app
from glob import glob
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType
from app.base_etl import BaseETL
from app.models import Weather, YieldData
from app.spark_helpers import table_to_df

MODEL_DATA_MAP = {
    "weather": "wx_data",
    "yield_data": "yld_data",
}
log = logging.getLogger(__name__)


def get_ingestor_list(target_model):
    """
    keeping the actual working directory access separate.
    This could be changed to a request to S3 or some nfs volume without changing the shape of the task info
    """
    data_dir = current_app.config["DATA_DIR"]
    model_dir = MODEL_DATA_MAP[target_model]
    files_path = f"{data_dir}/{model_dir}/*.txt"
    files = glob(files_path)

    return files


def make_wx_data_ingestion_tasks() -> list:
    """
    A task in this context is essentially a file name. In a funtional data pipeline,
    the target directory will be populated by a separate downloader worker.
    """
    tasks = get_ingestor_list("weather")
    return tasks


def make_yield_data_ingestion_tasks() -> list:
    """
    In this example, yield_data is only one file, but let's make sure it works if that were to change
    As we learn about the data, there could be more metadata returned about specific ingestion tasks,
    or these could be skipped and removed.
    """
    tasks = get_ingestor_list("yield_data")
    return tasks


def weather_formatter(df):
    """
    After import date must be reformatted
    station_code is extracted from the input file path
    dtypes re-cast to correct types
    :param df: spark dataframe
    :return:   spark dataframe
    """
    df = df.withColumn("station_code", fn.split(fn.input_file_name(), "/"))
    df = df.withColumn(
        "station_code",
        fn.split(fn.col("station_code")[fn.size("station_code") - 1], r"\.")[0],
    )
    df = df.withColumn("date", fn.to_date(fn.col("date"), "yyyyMMdd"))
    df = (
        df.withColumn(
            "max_temp",
            fn.when(df.max_temp == "-9999", None).otherwise(
                fn.col("max_temp").cast(IntegerType())
            ),
        )
        .withColumn(
            "min_temp",
            fn.when(df.min_temp == "-9999", None).otherwise(
                fn.col("min_temp").cast(IntegerType())
            ),
        )
        .withColumn(
            "precipitation",
            fn.when(df.precipitation == "-9999", None).otherwise(
                fn.col("precipitation").cast(IntegerType())
            ),
        )
    )

    # df = (
    #     df.withColumn("max_temp", fn.regexp_replace("max_temp", -9999, None))
    #     .withColumn("min_temp", fn.regexp_replace("min_temp", -9999, None))
    #     .withColumn("precipitation", fn.regexp_replace("precipitation", -9999, None))
    # )

    return df


def yield_data_formatter(df):
    """
    After csv load, add country and crop type from filename
    cast year and yield value to int
    :param df: spark dataframe
    :return:   spark dataframe
    """
    # add country & crop
    df = df.withColumn("filepath", fn.split(fn.input_file_name(), "/"))
    df = df.withColumn(
        "country", fn.split(fn.col("filepath")[fn.size("filepath") - 1], "_")[0]
    )
    df = df.withColumn(
        "crop", fn.split(fn.col("filepath")[fn.size("filepath") - 1], "_")[1]
    )
    df = df.drop("filepath")
    df = df.withColumn("year", fn.col("year").cast(IntegerType()))
    df = df.withColumn("k_metric_tons", fn.col("k_metric_tons").cast(IntegerType()))
    return df


class WeatherETL(BaseETL):
    def __init__(self):
        super().__init__(
            collector=make_wx_data_ingestion_tasks,
            formatter=weather_formatter,
            db_model=Weather,
        )
        self.loader = self.weather_loader

    def weather_loader(self, df):
        custom_schema = (
            Weather.sqlite_read_schema()
            if current_app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite")
            else None
        )
        # check if any data exists or we get errors from jdbc
        first = Weather.query.first()
        if first:
            cur = table_to_df(Weather.__tablename__, custom_schema)
            cur = cur.select(
                "date", "max_temp", "min_temp", "precipitation", "station_code"
            )
            df = df.subtract(cur)

        return self.spark_db_write(df)


class YieldETL(BaseETL):
    def __init__(self):
        super().__init__(
            collector=make_yield_data_ingestion_tasks,
            formatter=yield_data_formatter,
            db_model=YieldData,
        )
        self.loader = self.yield_data_loader

    def yield_data_loader(self, df):
        first = YieldData.query.first()
        if first:
            cur = table_to_df(YieldData.__tablename__)
            cur = cur.select("year", "k_metric_tons", "country", "crop")
            df = df.subtract(cur)

        return self.spark_db_write(df)
