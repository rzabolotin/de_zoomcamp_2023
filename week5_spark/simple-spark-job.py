from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

#write to hive
spark.range(100).select(F.sum('id').alias("id")).write.mode('overwrite').saveAsTable('test_table')
