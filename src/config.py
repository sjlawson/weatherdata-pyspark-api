"""This is adapted from a flask config template I've used for years. I like that I can adapt it to funtion in multiple environments. I think with a little more tweaking, setting up alternate jdbc connections for Spark here would clean up some code in the app module"""

import os

basedir = os.path.abspath(os.path.dirname(__file__))
SQLITE_PATH = os.path.join(basedir, "data-dev.sqlite")
SQLITE_JDBC_PATH = os.path.join(basedir, "jars/sqlite-jdbc-3.34.0.jar")
MYSQL_JDBC_PATH = os.path.join(basedir, "jars/mysql-connector-j-8.0.32.jar")


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "sjlawson-weatherdata-proof-of-concept"
    SSL_REDIRECT = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_RECORD_QUERIES = True
    WORKING_DIR = "/tmp/weatherdata"
    PACKAGE_DIRECTORY = os.path.join(basedir, "jars")

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True
    DATA_DIR = os.path.realpath("..") + "/data_dir"
    dev_db_url = os.environ.get("DEV_DATABASE_URL")
    SQLALCHEMY_DATABASE_URI = dev_db_url or f"sqlite:///{SQLITE_PATH}"
    SPARK_DB_CON = f"jdbc:{dev_db_url}" if dev_db_url else f"jdbc:sqlite:{SQLITE_PATH}"
    SPARK_DB_DRIVER = MYSQL_JDBC_PATH if dev_db_url else SQLITE_JDBC_PATH


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(basedir, "data-test.sqlite")
    WTF_CSRF_ENABLED = False
    DATA_DIR = os.path.realpath("./tests/sample_data")
    SPARK_DB_CON = "jdbc:sqlite:" + os.path.join(basedir, "data-test.sqlite")
    SPARK_DB_DRIVER = SQLITE_JDBC_PATH


class ProductionConfig(Config):
    db_url = os.environ.get("DATABASE_URL")
    sqlite_fallback = os.path.join(basedir, "data.sqlite")
    SQLALCHEMY_DATABASE_URI = db_url or "sqlite:///" + sqlite_fallback
    SPARK_DB_CON = f"jdbc:{dev_db_url}" if db_url else f"jdbc:sqlite:{sqlite_fallback}"
    SPARK_DB_DRIVER = MYSQL_JDBC_PATH if db_url else SQLITE_JDBC_PATH
    DATA_DIR = os.path.realpath("..") + "/data_dir"

    @classmethod
    def init_app(cls, app):
        Config.init_app(app)


config = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
    "default": DevelopmentConfig,
}
