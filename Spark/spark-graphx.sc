-- start the spark shell by loading graph frames package

spark-shell --packages graphframes:graphframes:0.5.0-spark2.2-s_2.11

val bikeStations = spark.read.option("header","true")
  .csv("/data/bike-data/201508_station_data.csv")
  
val tripData = spark.read.option("header","true")
  .csv("/data/bike-data/201508_trip_data.csv")
  

val stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()

val tripEdges = tripData
  .withColumnRenamed("Start Station", "src")
  .withColumnRenamed("End Station", "dst")
  
import org.graphframes.GraphFrame

-- creating the graph frame object

val stationGraph = GraphFrame(stationVertices, tripEdges)
stationGraph.cache()

-- printing the statistics of the graph

println(s"Total Number of Stations: ${stationGraph.vertices.count()}")
println(s"Total Number of Trips in Graph: ${stationGraph.edges.count()}")
println(s"Total Number of Trips in Original Data: ${tripData.count()}")

-- querying the graph

import org.apache.spark.sql.functions.desc
stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)


