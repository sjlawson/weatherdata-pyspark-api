from sqlalchemy import extract
from app.models import Weather, Analysis, YieldData


def weather_api_query(page, page_size, station_code=None, date=None):
    query = Weather.query
    if station_code:
        query = query.filter(Weather.station_code == station_code)

    if date and len(date) == 4:
        query = query.filter(extract("year", Weather.date) == date)
    elif date and len(date) == 10:  # expect format yyyy-dd-MM OR just yyyy
        try:
            query = query.filter(Weather.date == date)
        except ValueError:
            log.error("Date format error")

    result = query.order_by(Weather.date.desc(), Weather.station_code).paginate(
        page=page, per_page=page_size
    )

    return result


def analysis_api_query(page, page_size, year=None):
    query = Analysis.query
    if year:
        query = query.filter(Analysis.year == year)

    result = query.order_by(Analysis.year).paginate(page=page, per_page=page_size)

    return result
