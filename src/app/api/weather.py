import logging
from http import HTTPStatus
from flask_restx import Resource, fields
from . import api_blueprint as api
from .api_query_helper import weather_api_query, analysis_api_query

log = logging.getLogger(__name__)
PAGE_SIZE = 40

weather_model = api.model(
    "Weather",
    {
        "uri": fields.Url("weather_ep"),
        "station": fields.String(
            default=None, description="Weather station code", required=False
        ),
        "date": fields.String(
            default=None,
            description="station record date/year (YYYY-MM-DD or YYYY)",
            required=False,
        ),
        "max_temp": fields.Integer,
        "min_temp": fields.Integer,
        "precipitation": fields.Integer,
    },
)

weather_parser = api.parser()
weather_parser.add_argument("station", type=str, location="args")
weather_parser.add_argument("date", type=str, location="args")
weather_parser.add_argument("page", type=int, location="args")

w_stats_model = api.model(
    "Analysis",
    {
        "uri": fields.Url("weather_stats_ep"),
        "year": fields.Integer,
        "avg_max_temp": fields.Float,
        "avg_min_temp": fields.Float,
        "total_precipitation": fields.Float,
    },
)

w_stats_parser = api.parser()
w_stats_parser.add_argument("year", type=int, location="args")
w_stats_parser.add_argument("page", type=int, location="args")


@api.route("/weather/", endpoint="weather_ep")
@api.response(HTTPStatus.OK.value, "Get weather data list")
class WeatherApi(Resource):
    @api.doc(model=weather_model)
    def get(self):
        args = weather_parser.parse_args()
        page = args.get("page", 1)
        station_code = args.get("station")
        date = args.get("date")

        data = weather_api_query(page, PAGE_SIZE, station_code, date)

        res = {
            "results": [
                {
                    "station_code": i.station_code,
                    "date": i.date.strftime("%Y-%m-%d"),
                    "max_temp": i.max_temp,
                    "min_temp": i.min_temp,
                    "precipitation": i.precipitation,
                }
                for i in data.items
            ],
            "pagination": {
                "count": data.total,
                "page": page,
                "per_page": PAGE_SIZE,
                "pages": data.pages,
            },
        }
        return res


@api.route("/weather/stats", endpoint="weather_stats_ep")
@api.response(HTTPStatus.OK.value, "Get weather statistics")
class WeatherStatsApi(Resource):
    @api.doc(model=w_stats_model)
    def get(self):
        args = w_stats_parser.parse_args()
        page = args.get("page", 1)
        year = args.get("year")

        data = analysis_api_query(page, PAGE_SIZE, year)

        res = {
            "results": [
                {
                    "year": i.year,
                    "avg_max_temp": i.avg_max_temp,
                    "avg_min_temp": i.avg_min_temp,
                    "total_precipitation": i.total_precipitation,
                }
                for i in data.items
            ],
            "pagination": {
                "count": data.total,
                "page": page,
                "per_page": PAGE_SIZE,
                "pages": data.pages,
            },
        }
        return res
