from dependency import create_spark_session

from pyspark.sql.functions import col, to_timestamp, concat, udf
from pyspark.sql.types import StringType, FloatType
from geopy.distance import geodesic


def main():
    spark = create_spark_session("aidetic-assignment")
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)
    spark.stop()
    return None


def extract_data(spark):
    """
    Load data from csv file format.

    Params
        spark: SparkSession
            Spark session object.

    Returns
        Spark DataFrame
    """
    columns_to_load = ["Date", "Time", "Latitude", "Longitude", "Type", "Depth", "Magnitude"]
    df = spark.read \
        .format("csv") \
        .option("inferSchema", False) \
        .option("header", True) \
        .option("path", "src/pyspark_aidetic_assignment/data/input.csv") \
        .load() \
        .select(columns_to_load) \
        .withColumn("Time", col("Time").cast(StringType())) \
        .withColumn("Latitude", col("Latitude").cast(FloatType())) \
        .withColumn("Longitude", col("Longitude").cast(FloatType())) \
        .withColumn("Depth", col("Depth").cast(FloatType())) \
        .withColumn("Magnitude", col("Magnitude").cast(FloatType()))
    return df


def transform_data(df):
    """
    Transform the dataset.
    Following transformations are done.
    1. Convert the Date and Time columns into a timestamp column named Timestamp
    2. Filter the dataset to include only earthquakes with a magnitude greater than 5.0.
    3. Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes
    4. Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).

    Params
        df: DataFrame
            Input DataFrame.
    Returns
        Transformed DataFrame.
    """
    df_transformed = df \
        .withColumn("Timestamp", to_timestamp(concat(col("Date"), col("Time")), 'MM/dd/yyyyHH:mm:ss')) \
        .drop("Date", "Time") \
        .filter(col('Magnitude') > 5.0) \
        .withColumn("Magnitude Level", categorizeEarthquake(col("Magnitude"))) \
        .withColumn("GodesicDistance(km)", getGodesicDistance(col("Latitude"), col("Longitude")))
    return df_transformed


def load_data(df):
    """
    Collect data locally and write to CSV.

    Params
        df: DataFrame
            Transformed DataFrame.
    """
    df.write.option("header", "true").csv("src/pyspark_aidetic_assignment/data/output")


@udf(returnType=StringType())
def categorizeEarthquake(magnitude):
    """
    Assumption
    ----------
    Low Level: Magnitude less than 6.0
    Moderate Level: Magnitude between 6.0 and 7.0
    High Level: Magnitude greater than 7.0
    """
    level = None
    if isinstance(magnitude, float):
        if magnitude < 6.0:
            level = "Low"
        elif magnitude > 7.0:
            level = "High"
        else:
            level = "Moderate"
    return level


@udf(returnType=FloatType())
def getGodesicDistance(latitude, longitude, reference_location=(0, 0)):
    if isinstance(latitude, float) and isinstance(longitude, float):
        return geodesic(reference_location, (latitude, longitude)).km
    return None


if __name__ == '__main__':
    main()
