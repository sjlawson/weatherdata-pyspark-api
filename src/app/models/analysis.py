import pytz
from datetime import datetime
from pyspark.sql.types import StructType
from app.spark_helpers import IntField, FloatField
from app.models.base_model import BaseModel
from app import db


class Analysis(BaseModel):
    __tablename__ = "analyses"
    year = db.Column(db.Integer, unique=True)
    avg_max_temp = db.Column(db.Float)
    avg_min_temp = db.Column(db.Float)
    total_precipitation = db.Column(db.Float)

    @staticmethod
    def sql_read_schema():
        """schema for importing table to spark"""
        return StructType(
            [
                IntField("year"),
                FloatField("avg_max_temp"),
                FloatField("avg_min_temp"),
                FloatField("total_precipitation"),
            ]
        )

    @staticmethod
    def sqlite_read_schema():
        return ""
