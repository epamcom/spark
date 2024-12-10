Project Name

A Spark ETL project to process and enrich restaurant data using PySpark, perform data validation and transformation, and save the results in Parquet format.

Features

Reads restaurant data and checks for missing latitude and longitude values.

Uses the OpenCage Geocoding API to map missing coordinates.

Joins restaurant data with weather data based on proximity.

Outputs enriched data in Parquet format.

Fully idempotent and optimized for local execution.

Requirements

Python 3.8+

PyCharm IDE (or any Python-supported IDE)

Apache Spark 3.x

OpenCage Geocoder API Key

Required Python libraries:

pyspark

requests

pandas

Setup

Clone the repository:

git clone <repository_url>
cd <repository_name>


Execution

Run the ETL process:

spark-submit main.py

The enriched data will be saved in the output/ directory in Parquet format.

Testing

Run unit tests to validate the implementation:

python -m unittest discover tests

Folder Structure

project_name/
|
|-- main.py                 # Entry point for the ETL job
|-- functions.py            # Helper functions
|-- test.py/                # Unit tests
|-- src/                    # Input datasets
|-- README.md               # Project documentation


Author

Nur-Akhmet Baimakhan



