import os
import logging
import click
from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
from flask_migrate import Migrate, upgrade
from app import create_app, db
from app.models import Weather, YieldData, Analysis
from app.ingestors import WeatherETL, YieldETL
from app.analytics import agg_weather_data
from app.spark_helpers import get_spark

log = logging.getLogger(__name__)

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


app = create_app(os.environ.get("FLASK_CONFIG", "development"))
migrate = Migrate(app, db)
CORS(app)


@app.shell_context_processor
def make_shell_context():
    return {}

@app.cli.command()
def load_data():
    app.spark = get_spark()
    log.info("Starting main ETL process...")
    weather_etl = WeatherETL()
    weather_etl.go_etl()
    yield_etl = YieldETL()
    yield_etl.go_etl()
    log.info("ETL processes complete.")


@app.cli.command()
def analyze():
    app.spark = get_spark()
    log.info("Starting aggregation and analysis...")
    agg_weather_data()
    log.info("Analysis process complete!")
