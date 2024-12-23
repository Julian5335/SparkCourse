from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)

lines = sc.textFile("file:///SparkCourse/section2/data/fakefriends.csv")
rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x: (x, int(1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))

results = averagesByAge.collect()
for result in results:
    print(result)