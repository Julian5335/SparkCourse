import re
from pyspark import SparkConf, SparkContext, RDD

def normalizeWords(text):
  return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///SparkCourse/section2/data/Book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, int(1))).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0]))

wordCountsSorted: RDD[tuple[int, str]] = wordCountsSorted.sortByKey() # type: ignore

for count, word in wordCountsSorted.collect():
  cleanWord = word.encode("ascii", "ignore")
  if (cleanWord):
    print(cleanWord.decode(), "\t", count)