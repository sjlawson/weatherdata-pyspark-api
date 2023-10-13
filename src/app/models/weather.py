from pyspark.sql.types import StructType
from app.spark_helpers import StringField, IntField, DateField
from app import db

from app.models.base_model import BaseModel


class Weather(BaseModel):
    __tablename__ = "weather_data"
    station_code = db.Column(db.String)
    date = db.Column(db.Date)
    max_temp = db.Column(db.Integer)
    min_temp = db.Column(db.Integer)
    precipitation = db.Column(db.Integer)
    __table_args__ = (db.UniqueConstraint(station_code, date, name="station_date_uc"),)

    @staticmethod
    def spark_schema():
        """
        Schema for reading the input csv
        use all StringField because spark csv reader reads all as string
        """
        return StructType(
            [
                StringField("date"),
                StringField("max_temp"),
                StringField("min_temp"),
                StringField("precipitation"),
            ]
        )

    @staticmethod
    def sql_read_schema():
        """schema for importing table to spark"""
        return StructType(
            [
                StringField("station_code"),
                DateField("date"),
                IntField("max_temp"),
                IntField("min_temp"),
                IntField("precipitation"),
            ]
        )

    @staticmethod
    def sqlite_read_schema():
        return "date STRING"
