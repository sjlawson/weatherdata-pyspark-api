from app.ingestors import (
    make_wx_data_ingestion_tasks,
    make_yield_data_ingestion_tasks,
    weather_formatter,
    yield_data_formatter,
)
from app.models import Weather, YieldData
import pytest


def get_weather_df(spark):
    df = spark.read.options(delimiter="\t").csv(
        "tests/sample_data/wx_data/", schema=Weather.spark_schema()
    )
    return df


def get_yld_data_df(spark):
    df = spark.read.options(delimiter="\t").csv(
        "tests/sample_data/yld_data/", schema=YieldData.spark_schema()
    )
    return df


def test_make_wx_data_ingestion_tasks(test_app):
    res = make_wx_data_ingestion_tasks()
    assert sorted(res)[0].endswith("USC00259090.txt")


def test_make_yield_data_ingestion_tasks(test_app):
    res = make_yield_data_ingestion_tasks()
    assert res[0].endswith("US_corn_grain_yield.txt")


def test_weather_formatter(test_app):
    df = get_weather_df(test_app.spark)
    null_max = df.where(df.max_temp == "-9999").count()
    assert null_max > 0
    assert df.columns == ["date", "max_temp", "min_temp", "precipitation"]
    df2 = weather_formatter(df)
    assert df2.columns == [
        "date",
        "max_temp",
        "min_temp",
        "precipitation",
        "station_code",
    ]
    assert df2.dtypes == [
        ("date", "date"),
        ("max_temp", "int"),
        ("min_temp", "int"),
        ("precipitation", "int"),
        ("station_code", "string"),
    ]
    assert df2.where(df2.max_temp == -9999).count() == 0
    assert df2.where(df2.min_temp == -9999).count() == 0
    assert df2.where(df2.precipitation == -9999).count() == 0


def test_yield_data_formatter(test_app):
    df = get_yld_data_df(test_app.spark)
    df2 = yield_data_formatter(df)
    assert df2.columns == ["year", "k_metric_tons", "country", "crop"]
    assert df2.dtypes == [
        ("year", "int"),
        ("k_metric_tons", "int"),
        ("country", "string"),
        ("crop", "string"),
    ]
