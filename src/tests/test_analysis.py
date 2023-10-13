import logging
import pytest
from app.ingestors import WeatherETL
from app.analytics import agg_weather_data


@pytest.mark.system
@pytest.mark.slow
def test_analysis(test_app, test_database, caplog):
    """This essentially runs the whole data pipeline"""

    caplog.clear()
    caplog.set_level(logging.INFO)
    weather_etl = WeatherETL()
    weather_etl.go_etl()
    agg_weather_data()
    log_messages = [_[-1] for _ in caplog.record_tuples]
    assert (
        "Completed aggregated analyses of weather data: 26 rows added"
        in log_messages
    )
