import sys
import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def main():
    postgres_table = str(sys.argv[1])   #"marts.shopping_list"
    url = str(sys.argv[2]) #"jdbc:postgresql://130.193.51.3:5432/postgres"
    pg_user = str(sys.argv[3]) #"jovyan"
    pg_password = str(sys.argv[4]) #"jovyan"
    files_to_load = str(sys.argv[5]) # ["/user/ubuntu/pro3/"] - list of files/folders to be loaded

    properties = {"user": pg_user,"password": pg_password,"driver": "org.postgresql.Driver"}
    mode = "append"

    spark = SparkSession.builder \
        .master("local") \
        .appName("Update_shopping_list") \
        .getOrCreate()

    events = spark.read.json(*files_to_load)
    window = Window().partitionBy('user_custom_id').orderBy('event_timestamp')

    events = events.select('user_custom_id','event_timestamp', 'page_url_path') \
        .withColumn("lead_1",F.lead("page_url_path", 1).over(window)) \
        .withColumn("lead_2",F.lead("page_url_path", 2).over(window)) \
        .withColumn("lead_3",F.lead("page_url_path", 3).over(window)) \
        .where(F.col("lead_3") == '/confirmation') \
        .where(F.col("lead_2") == '/payment') \
        .where(F.col("lead_1") == '/cart') \
        .where(F.substring(F.col("page_url_path"),0,8)=='/product') \
        .select('user_custom_id','event_timestamp', 'page_url_path')

    # Чтобы случайно не загрузить данные, которые уже загружены:
    test = spark.read.jdbc(url=url, table=postgres_table, properties=properties)

    result = events.join(test,(events.user_custom_id  == test.user_custom_id) \
                     & (events.event_timestamp  == test.event_timestamp) \
                     & (events.page_url_path  == test.page_url_path),
                     how = 'leftanti') \
                .select(events.user_custom_id, events.event_timestamp, events.page_url_path)

    # Запишем только новые строки в БД
    result.write.jdbc(url=url, table=postgres_table, mode=mode, properties=properties)

if __name__ == "__main__":
    main()