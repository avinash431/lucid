-- create a normal static df

val static = spark.read.json("/data/activity-data/")
val dataSchema = static.schema

-- create a straming structured df

val streaming = spark.readStream.schema(dataSchema)
  .option("maxFilesPerTrigger", 1).json("/data/activity-data")

val activityCounts = streaming.groupBy("gt").count()

spark.conf.set("spark.sql.shuffle.partitions", 5)

val activityQuery = activityCounts.writeStream.queryName("activity_counts")
  .format("memory").outputMode("complete")
  .start()
  
spark.streams.active

for( i <- 1 to 5 ) {
    spark.sql("SELECT * FROM activity_counts").show()
    Thread.sleep(1000)
}


-- transformation on streaming dataframe

import org.apache.spark.sql.functions.expr
val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
  .where("stairs")
  .where("gt is not null")
  .select("gt", "model", "arrival_time", "creation_time")
  .writeStream
  .queryName("simple_transform")
  .format("memory")
  .outputMode("append")
  .start()
  
-- aggregations on

val deviceModelStats = streaming.cube("gt", "model").avg()
  .drop("avg(Arrival_time)")
  .drop("avg(Creation_Time)")
  .drop("avg(Index)")
  .writeStream.queryName("device_counts").format("memory").outputMode("complete")
  .start()
  
 spark.sql("select * from device_counts").show
 
 
  
 
  
