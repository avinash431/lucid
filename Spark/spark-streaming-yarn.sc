import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 

val ssc = new StreamingContext(sc, Seconds(1))

val lines = ssc.socketTextStream("localhost", 9999)

val words = lines.flatMap(_.split(" "))

val pairs = words.map(word => (word, 1))

val wordCounts = pairs.reduceByKey(_ + _)

wordCounts.print()

ssc.start()    

-- open another terminal and run the below command

nc -lk 9999

-- enter the text and the in spark terminal you'll see the word count 

--stateful transformation

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
 
      val previousCount = state.getOrElse(0)
 
      Some(currentCount + previousCount)
    }

val ssc = new StreamingContext(sc, Seconds(1))

-- checkpoint the Dstream RDD
ssc.checkpoint("hdfs://quickstart.cloudera.com/user/training/spark/checkpoint")

val lines = ssc.socketTextStream("localhost", 9999)

val words = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_).updateStateByKey(updateFunc)

words.print()

ssc.start()    

-- open another terminal and run the below command

nc -lk 9999

-- enter the text and the in spark terminal you'll see the word count 


 -- sliding window function

import org.apache.spark.streaming._

 val stc = new StreamingContext(sc, Seconds(3))

stc.checkpoint("hdfs://quickstart.cloudera.com/user/training/spark/checkpoint")

val lines = stc.socketTextStream("localhost", 9999)

val words = lines.flatMap(_.split(" "))

val pairs = words.map(word => (word, 1))

val wordCounts = pairs.reduceByKeyAndWindow(((x:Int, y:Int) => x + y), 
                                             Seconds(15), Seconds(3))

wordCounts.print

stc.start()

-- open another terminal and run the below command

nc -lk 9999

-- enter the text and the in spark terminal you'll see the word count 


