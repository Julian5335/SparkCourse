from pyspark.sql import SparkSession, functions as func

spark: SparkSession = SparkSession.builder.appName("WordCount").getOrCreate() # type: ignore

inputDF = spark.read.text("file:///SparkCourse/section3/data/Book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

wordCountsSorted = wordsWithoutEmptyString\
  .select(func.lower(wordsWithoutEmptyString.word).alias("word"))\
  .groupBy("word").count()\
  .sort("count")

totalRows = wordCountsSorted.count()
wordCountsSorted.show(totalRows)

spark.stop()