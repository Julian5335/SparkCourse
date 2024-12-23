from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
  movieNames = {}
  with codecs.open(\
    "C:/SparkCourse/section4/data/ml-100k/u.ITEM",\
    "r",\
    encoding = "ISO-8859-1",\
    errors = "ignore"\
  ) as f:
    for line in f:
      fields = line.split("|")
      movieNames[int(fields[0])] = fields[1]
  return movieNames

spark: SparkSession = SparkSession.builder.appName("PopularMovies").getOrCreate() # type: ignore

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType([\
  StructField("userID", IntegerType(), True),\
  StructField("movieID", IntegerType(), True),\
  StructField("rating", IntegerType(), True),\
  StructField("timestamp", LongType(), True),\
])

# Load up movie as dataframe
table = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/section4/data/ml-100k/u.data")

movieCounts = table.groupBy("movieID").count()

# Create a user defined function (udf) to look up movie names from our broadcasted disctionary
def lookupName(movieID):
  return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

moviesWithNames\
  .orderBy(func.desc("count"))\
  .show(10, False)

spark.stop()