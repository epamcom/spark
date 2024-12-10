import os
import requests
import geohash
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

BASE_URL = "https://api.opencagedata.com/geocode/v1/json"
csv_folder_path = r"src/restaurant_csv/restaurant_csv/"
#csv_files = [os.path.join(csv_folder_path, f) for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
csv_files = "src/restaurant_csv/restaurant_csv/part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
parquet_folder_path_1 = r"src/1_October_1-4/weather/year=2016/month=10/day=01/"
parquet_folder_path_2 = r"src/1_October_1-4/weather/year=2016/month=10/day=02/"
parquet_folder_path_3 = r"src/1_October_1-4/weather/year=2016/month=10/day=03/"
parquet_folder_path_4 = r"src/1_October_1-4/weather/year=2016/month=10/day=04/"

parquet_files_1 = [os.path.join(parquet_folder_path_1, f)
                   for f in os.listdir(parquet_folder_path_1) if f.endswith('.parquet')]
parquet_files_2 = [os.path.join(parquet_folder_path_2, f)
                   for f in os.listdir(parquet_folder_path_2) if f.endswith('.parquet')]
parquet_files_3 = [os.path.join(parquet_folder_path_3, f)
                   for f in os.listdir(parquet_folder_path_3) if f.endswith('.parquet')]
parquet_files_4 = [os.path.join(parquet_folder_path_4, f)
                   for f in os.listdir(parquet_folder_path_4) if f.endswith('.parquet')]
parquet_files = parquet_files_1 + parquet_files_2 + parquet_files_3 + parquet_files_4


def get_lat_lon_from_address(address, api_key="4b7c3ceb81194268b1f6a03d3910f681"):
    """Fetch latitude and longitude from OpenCage Geocoding API."""
    url = f'https://api.opencagedata.com/geocode/v1/json?q={address}&key={api_key}'
    response = requests.get(url)
    data = response.json()

    if data['results']:
        lat = data['results'][0]['geometry']['lat']
        lon = data['results'][0]['geometry']['lng']
        return lat, lon
    else:
        return None, None


@pandas_udf(StringType())
def generate_geohash(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return lat.combine(lon, lambda l, g: geohash.encode(l, g)[:4] if pd.notnull(l) and pd.notnull(g) else None)



