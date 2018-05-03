-- local mode

-- create RDD's from in-memory

val x = sc.parallelize(List(1,2,3,4,5))

-- map transformation and collect,count action

val x_map = x.map(x => x+ 1)

x_map.collect()

x_map.count()

-- to get the lineage of a RDD

x_map.toDebugString

-- to get the number of  partitions of the RDD

x_map.getNumPartitions

--- filter transformation and reduce action
val nums = 1 to 1000

vam nums_rdd = sc.parallelize(nums)

val nums_filter_rdd = nums_rdd.filter(x => x%2 ==0)

nums_filter_rdd.collect()

val nums_even_count = nums_filter_rdd.reduce((x,y) => x+y)

-- map vs flatmap transformation
sc.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect()

sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect()

-- mapPartitions and mapPartitionsWithIndex transformation

val parallel = sc.parallelize(1 to 9)
parallel.mapPartitions( x => List(x.next).iterator).collect

parallel.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect

val parallel = sc.parallelize(1 to 9, 3)
parallel.mapPartitions( x => List(x.next).iterator).collect

parallel.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect


-- create RDD's from local file system and pair RDD's

val babyNames = sc.textFile("baby_names.csv")
val rows = babyNames.map(line => line.split(","))

val namesToCounties = rows.map(name => (name(1),name(2)))

namesToCounties.groupByKey.collect

val filteredRows = babyNames.filter(line => !line.contains("Count")).map(line => line.split(","))
filteredRows.map(n => (n(1),n(4).toInt)).reduceByKey((v1,v2) => v1 + v2).collect
filteredRows.map ( n => (n(1), n(4))).aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v).sortBy(_._2).collect
filteredRows.map ( n => (n(1), n(4))).sortByKey().foreach (println _)
filteredRows.map ( n => (n(1), n(4))).sortByKey(false).foreach (println _)

 
