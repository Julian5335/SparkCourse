from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("FakeFriendsHeader").getOrCreate() # type: ignore

people = spark.read.option("header", "true").option("inferSchema", "true")\
  .csv("file:///SparkCourse/section3/data/fakefriends-header.csv")
  
print("here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years or older")
people.select(people.name, people.age + 10).show()

spark.stop()