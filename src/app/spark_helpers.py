import pyspark.pandas as ps
from flask import current_app
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import StructField, IntegerType, DateType, StringType, FloatType
from app import db


def _common_config(conf):
    jars = [
        "%s/%s" % (current_app.config["PACKAGE_DIRECTORY"], _)
        for _ in ["sqlite-jdbc-3.34.0.jar", "mysql-connector-j-8.0.32.jar"]
    ]
    conf = (
        conf.set("spark.python.worker.memory", "128m")
        .set("spark.task.maxFailures", "10")
        .set("spark.rdd.compress", "true")
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .set("spark.jars", ",".join(jars))
        .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.enabled", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.sql.broadcastTimeout", 600)
        .set("spark.network.timeout", "240s")
        .set("spark.driver.extraClassPath", current_app.config["SPARK_DB_DRIVER"])
        .set("spark.executor.extraClassPath", current_app.config["SPARK_DB_DRIVER"])
    )
    return conf


def _spark_config(app_name, num_cores, driver_memory):
    conf = (
        SparkConf()
        .setAppName(app_name)
        .setMaster("local[%s,4]" % num_cores)
        .set("spark.driver.memory", driver_memory)
    )
    return _common_config(conf)


def get_spark(
    app_name="weatherdata-api",
    num_cores=2,
    driver_memory="8g",
):
    conf = _spark_config(app_name, num_cores, driver_memory)
    spark_builder = SparkSession.builder.config(conf=conf)
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


class StringField(StructField):
    def __init__(self, field_name, nullable=True):
        super().__init__(name=field_name, dataType=StringType())
        self.nullable = nullable


class IntField(StructField):
    def __init__(self, field_name, nullable=True):
        super().__init__(name=field_name, dataType=IntegerType())
        self.nullable = nullable


class DateField(StructField):
    def __init__(self, field_name, nullable=True):
        super().__init__(name=field_name, dataType=DateType())
        self.nullable = nullable


class FloatField(StructField):
    def __init__(self, field_name, nullable=True):
        super().__init__(name=field_name, dataType=FloatType())
        self.nullable = nullable


def table_to_df(tablename, custom_schema=None):
    """Lesson learned:
    Since sqlite was chosen as a test option, this gets messy here because
    spark/pandas can't import date fields from sqlite.
    TODO: drop sqlite and deploy with mysql container even for tests and dev
    """
    options = (
        {
            "customSchema": custom_schema,
        }
        if custom_schema
        else None
    )

    if custom_schema:
        psdf = ps.read_sql(
            tablename, con=current_app.config["SPARK_DB_CON"], options=options
        )
    else:
        psdf = ps.read_sql(tablename, con=current_app.config["SPARK_DB_CON"])

    df = psdf.to_spark(index_col=None)
    if custom_schema and "date" in custom_schema:
        df = df.withColumn("date", fn.to_date(fn.col("date"), "yyyy-MM-dd"))

    return df


def df_to_db(df, tablename, if_exists="append"):
    """
    This looks like and antipattern because it is.
    It's needed for this demo because I'm using sqlite which
    won't allow multiple write connections
    """

    df.toPandas().to_sql(tablename, con=db.engine, if_exists=if_exists, index=False)
