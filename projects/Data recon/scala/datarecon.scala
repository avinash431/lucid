import java.io.File
import java.util.Properties

import scala.collection.immutable.Map
import scala.util.parsing.json.JSON

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object DataRecon {

  val conf: SparkConf = new SparkConf().setAppName("DataRecon")
  val sc: SparkContext = new SparkContext()
  val sqlContext: SQLContext = new SQLContext(sc)
  val hqlC: HiveContext = new HiveContext(sc)
  var df: DataFrame = null
  var temp_df: DataFrame = null
  var current_cob_date: String = null
  var previous_cob_date: String = null
  var table_df: DataFrame = null
  var table_columns: Array[String] = null;
  var cob_query: String = null
  var employees_table: DataFrame = null;
  var dbName: String = ""
  var maria_tbname: String = null
  var jdbcUrl: String = null
  var config: Config = null
  var flag: Boolean = true
  val connectionProperties = new Properties()
  val get_cob_date: String = null;
  var totalPartitionsDF: DataFrame = null;
  var cc: DataValidation = null

  def read_properties() {

    val jdbcHostname = config.getString("mariadb_hostname")
    val jdbcPort = config.getInt("mariadb_port")
    val jdbcDatabase = config.getString("mariadb_name")
    val jdbcUsername = config.getString("mariadb_username")
    val jdbcPassword = config.getString("mariadb_pwd")

    maria_tbname = config.getString("mariadb_table_name")
    jdbcUrl = s"jdbc:mariadb://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    dbName = config.getString("database_name")

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

  }

  def create_DataFrame() {

    val schema = StructType(
      StructField("source_name", StringType, false) ::
        StructField("table_name", StringType, false) ::
        StructField("previous_cob_date", StringType, false) ::
        StructField("current_cob_date", StringType, false) ::
        StructField("previous_count", StringType, false) ::
        StructField("current_count", StringType, false) ::       
        StructField("validation_type", StringType, false) ::
        StructField("remarks", StringType, false) :: Nil)

    df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

  }

  def check_cob_date(table_columns: Array[String]): Boolean = {

    if (table_columns contains "cob_date") true else false

  }

  def fetch_cob_dates_dataFrame(df: DataFrame): DataFrame = {

    var partitionDF: DataFrame = df

    partitionDF = partitionDF.select("cob_date").distinct

    partitionDF = partitionDF.select("cob_date").orderBy(partitionDF("cob_date").desc)

    partitionDF
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {

      throw new Exception("Please provide arguments properly")
      // ./DataRecon.scala <source-name:[list of tables]> <prop-file>
    }

    var arg: String = args(0)
    val prop_file = args(1)

    config = ConfigFactory.parseFile(new File(prop_file))

    arg = arg.replace(''', '"')

    val source_files_list: Map[String, List[String]] = JSON.parseFull(arg).get.asInstanceOf[Map[String, List[String]]]

    create_DataFrame()

    read_properties()

    val get_cobDate = (x: String) => if (x contains "/") x.split("/")(0).split("=")(1) else x.split("=")(1)

    val get_cob_date = udf(get_cobDate)
    
    hqlC.sql("set hive.execution.engine=spark");

    for ((source_name, tableNames) <- source_files_list) {
      for (tableName <- tableNames) {

        println(s"--------------Source Name is $source_name and table name is $tableName")

        cc = new DataValidation(source_name, tableName)

        val column_query = s"describe $dbName.$tableName"

        val column_df = hqlC.sql(column_query)

        val table_columns: Array[String] = column_df.rdd.map(x => x(0).toString).collect

        if (check_cob_date(table_columns)) {

          cob_query = s"show partitions $dbName.$tableName"

          totalPartitionsDF = hqlC.sql(cob_query)

          import sqlContext.implicits._

          totalPartitionsDF = totalPartitionsDF.withColumn("cob_date", get_cob_date($"result"))

          val partitionDF = fetch_cob_dates_dataFrame(totalPartitionsDF)

          val partitionDF_count = partitionDF.count

          if (partitionDF_count == 1) {
            current_cob_date = partitionDF.first().getString(0)
            previous_cob_date = "NA"

          } else if (partitionDF_count == 0) {
            println("no cob dates found for this table")
            flag = false

          } else {
            current_cob_date = partitionDF.first().getString(0)
            previous_cob_date = partitionDF.rdd.take(2)(1).getString(0)

          }
          if (flag) {
            println(s"Current cob date is $current_cob_date")
            println(s"Previous cob date is $previous_cob_date")
            temp_df = cc.create_dataFrame_cob_dates(current_cob_date, previous_cob_date)
            df = df.unionAll(temp_df)
          }
        } else {
          println(s"table {tableName} doesn't contain the column cob date")
        }
      }
    }

    df = df.withColumn("process_date", current_date())
   
    df.write.mode("append").jdbc(jdbcUrl, maria_tbname, connectionProperties)
  }

}
