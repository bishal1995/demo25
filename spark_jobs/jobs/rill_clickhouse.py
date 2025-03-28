import sys
from pathlib import Path
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, hour, unix_timestamp


def download_files(data_url, lookup_url, download_dir):
    """
    Download source data and lookup files.

    Args:
        data_url (str): URL for the main data parquet file
        lookup_url (str): URL for the lookup CSV file
        download_dir (Path): Directory to save downloaded files

    Returns:
        tuple: Paths to downloaded data and lookup files
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    data_download_path = download_dir / "sample.parquet"
    lookup_download_path = download_dir / "lookup.csv"

    urllib.request.urlretrieve(data_url, str(data_download_path))
    urllib.request.urlretrieve(lookup_url, str(lookup_download_path))

    return data_download_path, lookup_download_path


def transform_data(spark, data_path, lookup_path):
    """
    Transform taxi trip data by joining with lookup data.

    Args:
        spark (SparkSession): Spark session
        data_path (Path): Path to parquet data file
        lookup_path (Path): Path to lookup CSV file

    Returns:
        DataFrame: Transformed data
    """
    # Read raw data and lookup data
    raw_data_df = spark.read.parquet(f"file://{data_path}")
    lookup_data_df = spark.read.option("header", "true").csv(f"file://{lookup_path}")

    # Transform data
    lookup_data_df_sm = lookup_data_df.select("LocationID", "Zone")

    # Join pickup and dropoff locations
    joined_pickup_data_df = raw_data_df.join(
        broadcast(lookup_data_df_sm),
        raw_data_df.PULocationID == lookup_data_df_sm.LocationID,
        'left'
    ).withColumnRenamed("Zone", "PickupZone").drop("LocationID")

    joined_final_data_df = joined_pickup_data_df.join(
        broadcast(lookup_data_df_sm),
        joined_pickup_data_df.DOLocationID == lookup_data_df_sm.LocationID,
        'left'
    ).withColumnRenamed("Zone", "DropZone").drop("LocationID")

    # Add additional transformations
    transformed_final_df = joined_final_data_df. \
        withColumn("trip_duration_in_sec",
                   (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))). \
        withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))

    return transformed_final_df


def write_to_clickhouse(transformed_df, clickhouse_url, clickhouse_properties):
    """
    Write transformed data to ClickHouse.

    Args:
        transformed_df (DataFrame): Transformed data
        clickhouse_url (str): JDBC URL for ClickHouse
        clickhouse_properties (dict): Connection properties
    """
    transformed_df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "taxi_trips") \
        .option("user", clickhouse_properties["user"]) \
        .option("password", clickhouse_properties["password"]) \
        .option("driver", clickhouse_properties["driver"]) \
        .option("createTableOptions", "engine=MergeTree() order by(VendorID)") \
        .mode("append") \
        .save()


def main():
    """
    Main ETL function to process taxi trip data and load into ClickHouse.
    """
    # Configuration
    JDBC_JAR_PATH = "/home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar"
    data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    download_dir = Path("/tmp")

    # ClickHouse connection properties
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "admin",
        "password": "password"
    }

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Parquet to ClickHouse") \
        .config("spark.jars", JDBC_JAR_PATH) \
        .getOrCreate()

    try:
        # Download source files
        data_download_path, lookup_download_path = download_files(data_url, lookup_url, download_dir)

        # Transform data
        transformed_df = transform_data(spark, data_download_path, lookup_download_path)

        # Write to ClickHouse
        write_to_clickhouse(transformed_df, clickhouse_url, clickhouse_properties)

        print("ETL process completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
