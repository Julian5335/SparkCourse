from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark: SparkSession = SparkSession.builder.appName("PopularMovies").getOrCreate() # type: ignore

schema = StructType([\
  StructField("userID", IntegerType(), True),\
  StructField("movieID", IntegerType(), True),\
  StructField("rating", IntegerType(), True),\
  StructField("timestamp", LongType(), True),\
])

table = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/section4/data/ml-100k/u.data")

table\
  .groupBy("movieID").count()\
  .orderBy(func.desc("count"))\
  .show(10)

spark.stop()