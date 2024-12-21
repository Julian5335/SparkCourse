from pyspark.sql import SparkSession, functions as func

spark: SparkSession = SparkSession.builder.appName("FriendsByAge").getOrCreate() # type: ignore

table = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/section3/data/fakefriends-header.csv")

friends = table.select("age", "friends")

friends\
  .groupBy("age")\
  .agg(func.round(func.avg("friends"), 2).alias("average friends"))\
  .sort("age")\
  .show()

spark.stop()