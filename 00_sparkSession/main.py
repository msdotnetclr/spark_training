from pyspark.sql import *
from app_lib.logger import Log4j

if __name__ == "__main__":
    print("Starting program")
    spark = (SparkSession
             .builder
             .master("local[3]")
             .appName("hello-spark")
             .getOrCreate())

    log = Log4j(spark)

    log.info(f"You are using Spark version ==== {spark.version} ====")

    # add app logic here

    log.info("Shutting down Spark")

    spark.stop()
