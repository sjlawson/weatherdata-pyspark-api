import pytest
from app import create_app, db
from app.spark_helpers import get_spark


@pytest.fixture(scope="module")
def test_app():
    app = create_app("testing")
    with app.app_context():
        spark = get_spark(app_name="testing")
        app.spark = spark
        yield app
        spark.stop()


@pytest.fixture(scope="module")
def test_database():
    db.create_all()
    yield db

    db.session.remove()
    db.drop_all()
