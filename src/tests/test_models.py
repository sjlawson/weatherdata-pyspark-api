from datetime import datetime
from app.models import Weather, Analysis, YieldData


def test_weather_model_create(test_app, test_database):
    weather = Weather(
        station_code="AAA0012345",
        date=datetime(2008, 7, 25),
        max_temp=400,
        min_temp=0,
        precipitation=50,
    )
    assert weather.station_code == "AAA0012345"
    weather.save()
    check_weather = Weather.query.where(Weather.station_code == "AAA0012345").first()
    assert check_weather.max_temp == weather.max_temp


def test_weather_orm_model_prevents_duplicates(test_app, test_database):
    """
    This verifies that duplicates can't be saved through the ORM,
    not necessarily through spark's jdbc connector.
    """
    from sqlalchemy.exc import IntegrityError

    weather = Weather(
        station_code="AAA0056789",
        date=datetime(2008, 7, 25),
        max_temp=400,
        min_temp=0,
        precipitation=501,
    )
    weather.save()
    weather2 = Weather(
        station_code="AAA0056789",
        date=datetime(2008, 7, 25),
        max_temp=400,
        min_temp=0,
        precipitation=501,
    )
    try:
        weather2.save()
    except IntegrityError as e:
        assert "UNIQUE constraint failed" in str(e.orig)

    weather_check = Weather.query.where(Weather.station_code == "AAA0056789").all()
    assert len(weather_check) == 1


def test_yield_data_model_create(test_app, test_database):
    yld = YieldData(year=2008, crop="corn grain", k_metric_tons=202112, country="CA")
    assert yld.k_metric_tons == 202112
    yld.save()
    check_yld = YieldData.query.where(YieldData.year == 2008).first()
    assert check_yld.crop == yld.crop


def test_analysis_model_create(test_app, test_database):
    analysis = Analysis(
        year=2112, avg_max_temp=45.2, avg_min_temp=-1.10001, total_precipitation=1100
    )
    assert analysis.year == 2112
    analysis.save()
    check_analysis = Analysis.query.where(Analysis.avg_max_temp == 45.2).first()
    assert check_analysis.avg_min_temp == analysis.avg_min_temp
