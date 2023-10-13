from pyspark.sql.types import StructType
from app.spark_helpers import StringField, IntField
from app import db
from app.models.base_model import BaseModel


class YieldData(BaseModel):
    __tablename__ = "yield_data"
    year = db.Column(db.Integer)
    crop = db.Column(db.String)
    k_metric_tons = db.Column(db.Integer)
    country = db.Column(db.String)

    @staticmethod
    def spark_schema():
        """Schema for reading csv"""
        return StructType(
            [
                StringField("year"),
                StringField("k_metric_tons"),
            ]
        )

    @staticmethod
    def sql_read_schema():
        """schema for importing table to spark"""
        return StructType(
            [
                IntField("year"),
                StringField("crop"),
                IntField("k_metric_tons"),
                StringField("country"),
            ]
        )

    @staticmethod
    def sqlite_read_schema():
        return ""
