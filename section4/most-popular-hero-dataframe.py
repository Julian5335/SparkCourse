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
  
connections.show()  

mostPopular: Row = connections.sort(func.col("connections").desc()).first() # type: ignore

mostPopularName: Row = names.filter(func.col("id") == mostPopular[0]).select("name").first() # type: ignore

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances")

spark.stop()