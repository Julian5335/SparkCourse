from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(",")
  stationId = str(fields[0])
  entryType = str(fields[2])
  temperature = int(fields[3])
  return (stationId, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/section2/data/1800.csv")
parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))

results = minTemps.collect()

for result in results:
  print(result[0], result[1])