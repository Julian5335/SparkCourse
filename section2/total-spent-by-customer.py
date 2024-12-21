from pyspark import SparkConf, SparkContext, RDD

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLines(line):
  fields = line.split(",")
  customerId = int(fields[0])
  amountSpent = float(fields[2])
  return (customerId, amountSpent)

lines = sc.textFile("file:///SparkCourse/section2/data/customer-orders.csv")
totalSpentByCustomer = lines.map(parseLines).reduceByKey(lambda x, y: x + y)
totalSpentByCustomerSorted: RDD[tuple[int, float]] = totalSpentByCustomer.map(lambda x: (x[1], x[0])).sortByKey() # type: ignore

for total, customer in totalSpentByCustomerSorted.collect():
  print(customer, round(total, 2))