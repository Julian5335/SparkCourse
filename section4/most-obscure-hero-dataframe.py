from pyspark.sql import SparkSession, functions as func, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark: SparkSession = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate() # type: ignore

schema = StructType([\
  StructField("id", IntegerType(), True),\
  StructField("name", StringType(), True)  
])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/section4/data/marvel-names.txt")

lines = spark.read.text("file:///SparkCourse/section4/data/marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
  .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)\
  .groupBy("id").agg(func.sum("connections").alias("connections"))
  
minConnectionCount: int = connections.agg(func.min("connections")).first()[0] # type: ignore

obscureHeroes = connections.filter(connections.connections == minConnectionCount)

obscureHeroes\
  .join(names, "id")\
  .select("name")\
  .show()

spark.stop()