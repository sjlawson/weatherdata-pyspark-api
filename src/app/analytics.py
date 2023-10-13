import logging
from flask import current_app
from app.spark_helpers import get_spark, table_to_df, df_to_db
from app.models import Weather, Analysis
from pyspark.sql import functions as fn

log = logging.getLogger(__name__)


def agg_weather_data():
    custom_schema = (
        Weather.sqlite_read_schema()
        if current_app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite")
        else None
    )
    first = Weather.query.first()
    if not first:
        log.error("Analysis: No data to analyze or connetion unavailable")
        return 1

    df = table_to_df(Weather.__tablename__, custom_schema)
    df = df.withColumn("year", fn.year(df.date))

    df_maxtemp = df.select("year", "max_temp").na.drop(how="any")
    df_mintemp = df.select("year", "min_temp").na.drop(how="any")
    df_precip = df.select("year", "precipitation").na.drop(how="any")

    df_agg = df_maxtemp.join(df_mintemp, ["year"], "outer").distinct()

    df_agg = df_agg.join(df_precip, ["year"], "outer").distinct()

    df_agg = (
        df.groupBy("year")
        .agg(
            # temps are in tenths of degrees C, so 100 == 10C
            fn.avg("max_temp") / fn.lit(10),
            fn.avg("min_temp") / fn.lit(10),
            # precipitation is in tenths of a mm, so 100 == 1 cm
            fn.sum("precipitation") / fn.lit(100),
        )
        .select(
            fn.col("year"),
            fn.col("(avg(max_temp) / 10)").alias("avg_max_temp"),
            fn.col("(avg(min_temp) / 10)").alias("avg_min_temp"),
            fn.col("(sum(precipitation) / 100)").alias("total_precipitation"),
        )
    )

    count = df_agg.count()
    rows = df_agg.collect()
    updates = 0
    adds = 0
    for row in rows:
        if cur := Analysis.query.filter(Analysis.year == row.year).first():
            updates += 1
            cur.avg_max_temp = row.avg_max_temp
            cur.avg_min_temp = row.avg_min_temp
            cur.total_precipitation = row.total_precipitation
        else:
            adds += 1
            cur = Analysis(
                year=row.year,
                avg_max_temp=row.avg_max_temp,
                avg_min_temp=row.avg_min_temp,
                total_precipitation=row.total_precipitation
            )
        cur.save()
    operations = [f"{adds} rows added"]
    if updates:
        operations.append(f"{updates} rows updated")
    op_msg = ", ".join(operations)
    log.info(f"Completed aggregated analyses of weather data: {op_msg}")
