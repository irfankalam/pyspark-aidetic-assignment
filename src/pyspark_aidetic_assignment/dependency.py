from pyspark.sql import SparkSession


def create_spark_session(app_name, local_mode=False):
    if local_mode:
        print("Started local mode")
        spark_session = SparkSession \
            .builder \
            .master("local[*]") \
            .appName(app_name) \
            .getOrCreate()
    else:
        print("Started for cluster")
        spark_session = SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()

    return spark_session
