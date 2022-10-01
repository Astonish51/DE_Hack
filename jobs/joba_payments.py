import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import findspark
import os
import datetime
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

def main():
    path_ = sys.argv[1]
    #date = sys.argv[2]
    table = sys.argv[2]
    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 
    findspark.init()
    findspark.find()
    spark = SparkSession \
        .builder \
        .master("local[*]") \
            .config("spark.driver.cores", "8") \
            .config("spark.driver.memory", "8g") \
            .appName(f"Lokhanov_pro3") \
            .getOrCreate()
    
    mode = "append"
    url = "jdbc:postgresql://130.193.51.3:5432/postgres"
    properties = {"user": "jovyan","password": "jovyan","driver": "org.postgresql.Driver"}
    
    path = path_
    result = spark.read.json(path).select('user_custom_id', 'event_timestamp', F.hour(F.col('event_timestamp')).alias('hour')).where("page_url_path=='/confirmation'")
    
    result.write.jdbc(url=url, table=table, mode=mode, properties=properties)
    
if __name__ == "__main__":
        main()