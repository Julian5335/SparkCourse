from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark: SparkSession = SparkSession.builder.appName("TotalSpent").getOrCreate() # type: ignore

schema = StructType([\
  StructField("customerId", IntegerType(), True), \
  StructField("orderId", IntegerType(), True), \
  StructField("price", FloatType(), True)
])

table = spark.read.schema(schema).csv("file:///SparkCourse/section3/data/customer-orders.csv")
table.printSchema()

totalByCustomerSorted = table\
  .select("customerId", "price")\
  .groupBy("customerId").agg(func.round(func.sum("price"), 2).alias("total spent"))\
  .orderBy("total spent")
  
totalCustomers = totalByCustomerSorted.count()
totalByCustomerSorted.show(totalCustomers)
  
spark.stop()