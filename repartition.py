
import sys
 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
 
def main():
        date = sys.argv[1]
        input_path = sys.argv[2]
        output_path = sys.argv[3]

        spark = SparkSession.builder \
                    .master("local") \
                    .appName(f"EventsPartitioningJob-{date}") \
                    .getOrCreate()
        events = spark.read.json(f"{input_path}/load_date={date}")\
                .withColumn('event_date', F.to_date('event_timestamp'))
        events\
            .repartition(1)\
            .write\
            .partitionBy('event_date', 'event_type')\
            .mode('overwrite')\
            .parquet(f'{output_path}')

if __name__ == "__main__":
        main()