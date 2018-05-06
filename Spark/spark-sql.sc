val myRange = spark.range(1000).toDF("number")

myRange

myRange.show

myRange.printSchema

val divisBy2 = myRange.where("number % 2 = 0")

divisBy2.count()

val flights = spark.read.option("inferSchema","true").option("header","true").csv("/Users/avinash/Desktop/flight_names.csv")

flights.show

flights.printSchema

flights.count

flights.take(10)

flights.sort("count").explain

val sort_flights = flights.sort("count")

sort_flights.show

spark.conf.set("spark.sql.shuffle.partitions", "5")

flights.sort("count").take(10)

sort_flights.createOrReplaceTempView("flights_2015")

-- count the no: of flights from a particular country
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flights_2015
GROUP BY DEST_COUNTRY_NAME
""")

val dataFrameWay = sort_flights.groupBy('DEST_COUNTRY_NAME).count

sqlWay.explain

dataFrameWay.explain

-- maximum value of the count 

spark.sql("SELECT max(count) from flights_2015").take(1)

import org.apache.spark.sql.functions.max

sort_flights.select(max("count")).take(1)

-- find the top five destination countries 

val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flights_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show

import org.apache.spark.sql.functions.desc

flights
  .groupBy("DEST_COUNTRY_NAME") 
  .sum("count") 
  .withColumnRenamed("sum(count)", "destination_total") 
  .sort(desc("destination_total")) 
  .limit(5) 
  .show()

flights
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .explain()



