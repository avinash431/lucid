-- create dataframe from memory

val myRange = spark.range(1000).toDF("number")

myRange

myRange.show

myRange.printSchema

df.first()

df.select(df.col("number"))

df.select(df.col("number") + 10)

df.select("number")

val divisBy2 = myRange.where("number % 2 = 0")

divisBy2.count()

val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

myDF.show

-- manually create Row

import org.apache.spark.sql.Row
val myRow = Row("Hello", null, 1, false)

myRow(0) // type Any
myRow(0).asInstanceOf[String] // String
myRow.getString(0) // String
myRow.getInt(2) // Int

-- manually create df from Row 

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))
val myRows = Seq(Row("Hello", null, 1L))
val myRDD = spark.sparkContext.parallelize(myRows)
val myDf = spark.createDataFrame(myRDD, myManualSchema)
myDf.show()



-- create dataframe from json file with automatic schema.

val df = spark.read.format("json")
  .load("/data/flight-data/json/2015-summary.json")

df.printSchema()

spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema

-- create df with manual schema 
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))

val df = spark.read.format("json").schema(myManualSchema)
  .load("/data/flight-data/json/2015-summary.json")

-- two ways to refer a column 

import org.apache.spark.sql.functions.{col, column}
col("someColumnName")
column("someColumnName")




-- create dataframe from csv file.

val df = spark.read.option("inferSchema","true").option("header","true").csv("/Users/avinash/Desktop/flight_names.csv")

spark.read.option("inferSchema","true").option("header","true").csv("/Users/avinash/Desktop/flight_names.csv").schema

df.show

df.printSchema

df.count

df.take(10)

-- access column via multiple ways

import org.apache.spark.sql.functions.{expr, col, column}

df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME,
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME"))
  .show(2)

-- expr is the most flexible way to access a column. example for renaming the column

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

-- literals

import org.apache.spark.sql.functions.lit
df.select(expr("*"), lit(1).as("One")).show(2)

-- adding column

df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
  .show(2)

-- renaming the column

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

-- drop the column

df.drop("ORIGIN_COUNTRY_NAME").columns

-- sort the df

df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

-- sort the df more specifically i.e asc, desc

import org.apache.spark.sql.functions.{desc, asc}

df.orderBy(expr("count desc")).show(2)
df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

-- no: of partitions

df.rdd.getNumPartitions

-- increase the no: of partition
df.repartition(5)

-- decrease the no: of partitions
df.coalesce(1)

-- partition the df based on the column

df.repartition(col("DEST_COUNTRY_NAME"))

df.repartition(5, col("DEST_COUNTRY_NAME"))


df.sort("count").explain

val sort_flights = df.sort("count")

sort_flights.show

spark.conf.set("spark.sql.shuffle.partitions", "5")

sort_flights.sort("count").take(10)

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

-- window function

val staticDataFrame = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema

import org.apache.spark.sql.functions.{window, column, desc, col}

staticDataFrame
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .show(5)


-- DataSet

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)
val flightsDF = spark.read
  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]

flights
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(flight_row => flight_row)
  .take(5)

flights
  .take(5)
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))



