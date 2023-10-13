from flask import Blueprint
import flask_restx

# app = Flask(__name__)
bp = Blueprint("api", __name__, url_prefix="/api")
api_blueprint = flask_restx.Api(
    bp,
    version="1.0",
    title="API",
    description="weather and yield data analysis api",
    doc="/doc/",
)

# app.register_blueprint(bp)
ns = api_blueprint.namespace("weather", description="Weather dataset queries")
api_blueprint.add_namespace(ns)

from . import weather
