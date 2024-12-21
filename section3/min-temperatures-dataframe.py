from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark: SparkSession = SparkSession.builder.appName("MinTemperatures").getOrCreate() # type: ignore

schema = StructType([\
  StructField("stationId", StringType(), True), \
  StructField("date", IntegerType(), True), \
  StructField("measure_type", StringType(), True), \
  StructField("temperature", FloatType(), True),
])

df = spark.read.schema(schema).csv("file:///SparkCourse/section3/data/1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")

minStationTemps = minTemps\
  .select("stationId", "temperature")\
  .groupBy("stationId").agg(func.min("temperature").alias("min_temp"))\
  .sort("min_temp")\
  .show()
  
spark.stop()