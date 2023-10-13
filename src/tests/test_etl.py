import logging
import pytest
from app.ingestors import WeatherETL, YieldETL


@pytest.mark.system
def test_weather_etl_process(test_app, test_database, caplog):
    caplog.clear()
    caplog.set_level(logging.INFO)
    weather_etl = WeatherETL()

    assert weather_etl.db_table == "weather_data"
    weather_etl.go_etl()
    log_messages = [_[-1] for _ in caplog.record_tuples]
    assert "Total records loaded: 18109" in log_messages

    # check that re-loading does not create duplicates
    caplog.clear()
    weather_etl.go_etl()
    log_messages = [_[-1] for _ in caplog.record_tuples]
    assert "Total records loaded: 0" in log_messages


def test_yield_data_etl_process(test_app, test_database, caplog):
    caplog.clear()
    caplog.set_level(logging.INFO)
    yield_etl = YieldETL()

    assert yield_etl.db_table == "yield_data"
    yield_etl.go_etl()
    log_messages = [_[-1] for _ in caplog.record_tuples]
    assert "Total records loaded: 30" in log_messages

    # check that re-loading does not create duplicates
    caplog.clear()
    yield_etl.go_etl()
    log_messages = [_[-1] for _ in caplog.record_tuples]
    assert "Total records loaded: 0" in log_messages
