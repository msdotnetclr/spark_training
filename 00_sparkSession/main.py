from pyspark.sql import *
from pyspark import SparkConf
from app_lib.logger import Log4j
from app_lib.utils import get_spark_app_config

if __name__ == "__main__":
    print("Starting program")
    conf = get_spark_app_config()
    spark = (SparkSession
             .builder
             .config(conf = conf)
             .getOrCreate())

    log = Log4j(spark)

    log.info(f"You are using Spark version ==== {spark.version} ====")

    # add app logic here

    conf_out = spark.sparkContext.getConf()
    log.info(conf_out.toDebugString())

    log.info("Shutting down Spark")

    spark.stop()
