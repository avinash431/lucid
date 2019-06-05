import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types._

class DataValidation(source_name: String, table_name: String) {

  val sc: SparkContext = DataRecon.sc
  val sqlContext: SQLContext = DataRecon.sqlContext
  var y_count: String = ""
  var validation_type: String = null
  val db_name: String = DataRecon.dbName
  val hqlC: HiveContext = DataRecon.hqlC
  var current_count: String = null
  var previous_count: String = null
  var validation_value: String = null
  var remarks: String = null
  var df: DataFrame = null
  var current_cob_date: String = null
  var previous_cob_date: String = null
  var return_df: DataFrame = null

  def create_empty_dataFrame(): Unit = {

    val schema = StructType(
      StructField("source_name", StringType, false) ::
        StructField("table_name", StringType, false) ::
        StructField("previous_cob_date", StringType, false) ::
        StructField("current_cob_date", StringType, false) ::
        StructField("previous_count", StringType, false) ::
        StructField("current_count", StringType, false) ::
        StructField("validation_type", StringType, false) ::
        StructField("remarks", StringType, false) :: Nil)

    return_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  }

  def count_check(y_count: String, t_count: String): DataFrame = {

    if (y_count == "NA" && t_count.toInt == 0)
      remarks = "warning"
    else if (y_count == "NA" && t_count.toInt > 0)
      remarks = "success"

    else if ((y_count.toInt == 0 && t_count.toInt > 0) || y_count.toInt == 0 && t_count.toInt == 0)
      remarks = "warning"
    else if (y_count.toInt > 0 && t_count.toInt == 0)
      remarks = "error"
    else
      remarks = "success"

    validation_type = "count check"

    val count_df = sqlContext.createDataFrame(Seq(
      (source_name, table_name, previous_cob_date, current_cob_date, y_count, t_count, validation_type, remarks)))
      .toDF("source_name", "table_name", "previous_cob_date", "current_cob_date", "previous_count", "current_count", "validation_type", "remarks")
    count_df
  }

  def create_dataFrame_cob_dates(c_cob_date: String, p_cob_date: String): DataFrame = {

    create_empty_dataFrame()

    current_cob_date = c_cob_date
    previous_cob_date = p_cob_date

    val c_query = s"select * from $db_name.$table_name where cob_date='$current_cob_date'"

    val current_DF = hqlC.sql(c_query)

    current_count = current_DF.count().toString

    if (previous_cob_date == "NA") {
      previous_count = "NA"
      remarks = "there is only one cob date for this table"

    } else {
      val p_query = s"select * from $db_name.$table_name where cob_date='$previous_cob_date'"
      val previous_DF = hqlC.sql(p_query)
      previous_count = previous_DF.count().toString
    }

    val count_check_df = count_check(previous_count, current_count)

    return_df = return_df.unionAll(count_check_df)

    return_df

  }

}
