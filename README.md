# Weather & Crop Yield data analysis and API

## Description:  
This is a small data application in Python that ingests a little over 1.7 million rows of weather data supplied by weather monitoring stations, transforms the rows into a local schema, and calculates a few common aggregations on temperature and precipitation. The data are then made avalaible through a very simple REST API.  

### Architecture features and Ideas for scale
Totally containerized using Docker/docker-compose, given a Kubernetes cluster, the docker-compose.yml could be converted to Kubernetes service and deployement files.  
Python 3.10, Flask API application  
API library: `flask_restx`  
Swagger api docs  
Database: SQLite3  
  - SQLite is used here for demonstration convenience. In a live production environment, I might use MySQL, Postgresql, and maybe even a nosql document store.
  - The data might require an indexer, and that could be stored as an ElasticSearch index
PySpark  
  - example of ingesting an entire collection of csv files in bulk.
  - Reads from DB into Spark dataframe, then runs some simple aggregations available in the spark dataframe api
  - Used for this as a proof-of-concept. This isn't an extremely large dataset, but the point of this API application is to be a template for larger data applications that would be most performant with Spark. 
  
### If I want to use AWS, S3, and EMR to process a much larger volume:
  - I wouldn't need to change much to modify the ingestors to load in bulk from S3 rather than a local data directory (and of course, add moto3). 
  - That would start with `app.ingestors.get_ingestor_list()`
  - From there one could take advantage of EMR's built-in capability to read data from S3 to partitioned RDD which can then be loaded directly into a spark dataframe.
  - Obviously other cloud data services (GCP, Azure) have different methods. Modifying this code to ingest data from AWS, GCP, Azure would involve building out connections to those services of course, but the actual ETL code would remain mostly unchanged.

## tl;dr  
If you have docker-compose installed, run this sample application  
from the program root, `src`, with a single command `docker-compose up`

## Installation and start-up
- clone this repository
- `cd weatherdata-pyspark-api/`
- `docker-compose up`
- ... wait as container builds and starts, runs data pipeline tasks: ingestion, and analysis
- run tests:
  - `docker exec -it weatherdata-pyspark-api_flask-app-server_1 pytest tests`
- check api in browser at urls:
  - http://0.0.0.0:5000/api/weather/
  - http://0.0.0.0:5000/api/weather/stats
  - http://0.0.0.0:5000/api/doc/
  - http://0.0.0.0:5000/api/swagger.json


### some more sample urls:
Date format can be: YYYY-MM-DD or YYYY - any other format is quietly ignored
`http://0.0.0.0:5000/api/weather/?date=2008-06-01`
`http://0.0.0.0:5000/api/weather/?date=2008`
`http://0.0.0.0:5000/api/weather/?date=2008&station=USC00110072`
`http://0.0.0.0:5000/api/weather/stats`
`http://0.0.0.0:5000/api/weather/stats?year=2005`

## Structure
Flask application with cli for ingestion and analysis tasks

`src` contains main flask app file `weather_data.py`
`data_dir` contains the two data sources, `wx_data` and `yld_data`

SQLAlchemy ORM is used for the data API layer
Pyspark handles ingestion and analysis

Default DB is SQLite3.
DB can be changed to MySQL by modifying `docker-compose.yml`
(has not been tested with MySQL)

## Description of ingestion process:
1. tab-separated files are loaded by pyspark
2. all data transformations ocurr within the sparkcontext
3. weather data gets loaded into the `weather_data` table through the pyspark.pandas API because it makes use of the SQLAlchemy engine as the db connection.
4. Loading from db to spark dataframe runs through the jdbc connector since it's likely to be a large volume
5. Analysis makes use of aggregation functions available in the pyspark.sql.functions module

### flask cli commands for ingesting and analysing data are:

`flask load-data`
`flask analyze`

If the app is deployed with docker, the ingestion/analysis scripts run automatically after start-up

## Deployment
build with docker from the repository root.

`docker-compose up`
On container startup, the `load-data` and `analyze` scripts will run as part of the `entrypoint.sh`

env vars:
If database url vars are not set, the app will just use sqlite3 for database.
Otherwise only MySQL should be used other than sqlite becuase I've only included those two JDBC jar files for Spark
`DATABASE_URL`
`DEV_DATABASE_URL`

## Wish list
If the data becomes larger than one host can manage, use EMR with docker maching configuration and modify ingestion and analysis to processes the data over multiple managed temporary instances, partitioned in Spark RDD dataframes

### Terraform
(not included in this setup)
First resources to consider:
- S3: store raw data sources rather than on server instace
- RDS: operational database
- EC2: linux instace to host
- EMR: future use if RDD is added


## Development
Python >= 3.10 required, venv recommended
`pip install -r requirements/dev.txt`
`black` is installed, but it's up to the dev to run it and keep their code clean and shiny

## Testing
Pytest is setup for this project.
You can run all tests with:

`pytest tests`
or in the running container with:
`docker exec -it weatherdata-pyspark-api_flask-app-server_1 pytest tests`

## API routes:
```
swagger api doc: /api/doc/
api.specs        /api/swagger.json
weather data     /api/weather/
weather_stats    /api/weather/stats
```
