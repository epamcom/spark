import requests

from pyspark.sql import SparkSession
from functions import parquet_files, csv_files, get_lat_lon_from_address, generate_geohash

# PART 1 - Data digestion

# Spark Session for ETL
spark = SparkSession.builder \
    .getOrCreate()


restaurant_df = spark.read.csv(
                csv_files,
                header=True,
                inferSchema=True,
                pathGlobFilter="*.csv")

weather_df = spark.read.parquet(
                *parquet_files,
                header=True,
                inferSchema=True, pathGlobFilter="*.parquet")

# Part 2 - Missing lat and lng
invalid_rows = restaurant_df.filter(restaurant_df.lat.isNull() | restaurant_df.lng.isNull())

missing_lat_lng = invalid_rows.collect()
#
# # Create a list to hold the updated rows
updated_rows = []
#
# Iterate through missing rows and update the lat/lng
for row in missing_lat_lng:
    address = f"{row['city']}%2C{row['country']}"

    lat, lng = get_lat_lon_from_address(address)

    # If we were able to get valid lat/lng, update it
    if lat and lng:
        updated_row = row.asDict()
        updated_row['lat'] = lat
        updated_row['lng'] = lng
        updated_rows.append(updated_row)

# # Convert the updated rows back to a DataFrame
updated_df = spark.createDataFrame(updated_rows, schema=restaurant_df.schema)

# Merge the updated rows back into the original DataFrame
final_df = restaurant_df.join(updated_df, on="id", how="left")
final_df.show()
# Part 3 - Use geohash
restaurant_df = restaurant_df.withColumn("geohash", generate_geohash(restaurant_df.lat, restaurant_df.lng))

joined_df = restaurant_df.join(weather_df, on='geohash', how='left')

joined_df.show()

# Part 4 - Write locally
joined_df.write.partitionBy("geohash").parquet("path_to_save_parquet_data")

