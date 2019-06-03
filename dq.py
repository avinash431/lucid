#!/usr/bin/spark2-submit

############################################################################
#
#                   Author  : Avinash Seethalam
#                   Date    : 09/04/2019
#                   Purpose : To capture the data quality metrics of di tables and populate them in maria db
#                   Usage   : python data_quality_metrics.py <di_tables_info>
#                             Calculates the below metrics
#                             Empty table check : Check table has 0 records or not
#                             Schema check : Check whether the column order and names are same as that of metadata
#                             Data type check : Check the values for integer/date data types based on metadata
#                             Numeric check : Check the precision of the numeric value based on the metadata
#                             Null check : Check the column has some value based on the metadata
#                             Duplicate check : Check the
#                             Date/timestamp check
#############################################################################

import time
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, SQLContext, DataFrameReader
from pyspark.sql.functions import col, lower, trim
from pyspark.conf import SparkConf
from pyspark.sql.types import StringType, DateType, StructType, StructField
from record_check import empty_records_check, schema_check, date_timestamp_check
from record_check import data_type_check, numeric_check, null_check, duplicate_check


# read the datarecon.config properties file
def read_properties_file(file_path, sep= "="):
    props = {}
    with open(file_path, "rt") as f:
        for line in f:
            if not line.startswith("#"):
                key_value = line.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


# insert the metric stats in mariadb
def insert_metrics_mariadb(sparksession, stats, maria_props, metrics_table_name):
    print "inserting in maria db, length of the stats is {0}".format(len(stats))
    schema = StructType([StructField("metric_date", StringType(), False),
                        StructField("source_name", StringType(), False),
                        StructField("table_name", StringType(), False),
                        StructField("hive_table_name", StringType(), False),
                        StructField("cob_date", StringType(), False),
                        StructField("metric_type", StringType(), False),
                        StructField("status", StringType(), False),
                        StructField("remarks", StringType(), True)])

    df = sparksession.createDataFrame(data=stats, schema=schema)

    mariadb_hostname = maria_props["mariadb_hostname"]
    mariadb_username = maria_props["mariadb_username"]
    mariadb_pwd = maria_props["mariadb_pwd"]
    mariadb_name = maria_props["mariadb_name"]
    mariadb_port = maria_props["mariadb_port"]
    url = "jdbc:mysql://{0}:{1}/{2}".format(mariadb_hostname, mariadb_port, mariadb_name)
    properties = {'user': mariadb_username, 'password': mariadb_pwd, 'driver': 'org.mariadb.jdbc.Driver'}

    df.write.mode("append").jdbc(url, table=metrics_table_name, properties=properties)


# create the spark conf parameters and returns the sparkconf object
def create_spark_conf(metric_name):
    sparkconf = SparkConf()
    sparkconf.setAppName(metric_name)
    sparkconf.set('spark.logConf', 'true')
    sparkconf.set("spark.executor.memoryOverhead", 4096)
    sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkconf.set("spark.kryo.unsafe", True)
    sparkconf.set("spark.io.compression.codec", "snappy")
    sparkconf.set("spark.sql.parquet.compression.codec", "snappy")
    sparkconf.set("spark.sql.caseSensitive", False)
    sparkconf.set("spark.shuffle.service.enabled", True)
    sparkconf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200")
    sparkconf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200")
    sparkconf.set("spark.sql.parquet.filterPushdown", True)
    sparkconf.set("spark.hadoop.parquet.filter.statistics.enabled", True)
    sparkconf.set("spark.hadoop.parquet.filter.dictionary.enabled", True)
    sparkconf.set("spark.hadoop.parquet.dictionary.page.size", 3145728)
    sparkconf.set("spark.sql.codegen.wholeStage", False)
    return sparkconf


# create spark session
def create_spark_session(sparkconf):
    try:
        sparksession = SparkSession.builder.config(conf=sparkconf).enableHiveSupport().getOrCreate()
        sparksession.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        print e
        sys.exit(1)
    else:
        return sparksession


# check hive di table
def check_hive_table(table_names, table_name):
    if table_name.lower() in table_names:
        return True


# fetch all the hive DI tables
def fetch_hive_tables(sparksession, database_name):
    sqlcontext = SQLContext(sparksession.sparkContext)
    table_names = sqlcontext.tableNames(database_name)
    table_names = list(map(lambda x: x.lower(), table_names))
    return table_names


# returns the corresponding hive table for the cob date as a Dataframe
def fetch_hive_table(sparksession, table_name, cob_date):
    hive_query = "select * from {0} where cob_date = '{1}'".format(table_name, cob_date)
    print "Hive query is {0}".format(hive_query)
    try:
        df = sparksession.sql(hive_query)
    except Exception as e:
        print e
        sys.exit(1)
    else:
        return df


# close the spark session
def close_spark_session(sparksession):
    try:
        sparksession.stop()
    except Exception as e:
        print e
        sys.exit(1)


# connect to source object structure marisdb table and returns the table as df
def get_source_object_structure_table(sparksession, maria_props, str_obj_table_name):
    mariadb_hostname = maria_props["mariadb_hostname"]
    mariadb_username = maria_props["mariadb_username"]
    mariadb_pwd = maria_props["mariadb_pwd"]
    mariadb_name = maria_props["mariadb_name"]
    mariadb_port = maria_props["mariadb_port"]
    url = "jdbc:mysql://{0}:{1}/{2}".format(mariadb_hostname, mariadb_port, mariadb_name)
    properties = {'user': mariadb_username, 'password': mariadb_pwd, 'driver': 'org.mariadb.jdbc.Driver'}
    sql_context = SQLContext(sparksession.sparkContext)
    df = DataFrameReader(sql_context).jdbc(url, table=str_obj_table_name, properties=properties)
    return df.select(lower(trim(col("Source_Object_ID"))).alias("Source_Object_ID"),lower(trim(col("Source_Object_Attribute_Name"))).alias("Source_Object_Attribute_Name"),lower(trim(col("Data_Type"))).alias("Data_Type"),trim(col("Length")).alias("Length"),trim(col("Scale")).alias("Scale"),trim(col("Position/Length")).alias("Position/Length"),trim(col("XML_Jason_path")).alias("XML_Jason_path"),trim(lower(col("Data_Pattern"))).alias("Data_Pattern"),col("Column_order"),lower(trim(col("Source_Unique_Key"))).alias("Source_Unique_Key"),lower(trim(col("Is_Mandatory"))).alias("Is_Mandatory"))


# get all the table names from source object structure table
def get_di_tables(df):
    return df.select("Source_Object_ID").distinct().select(lower(col("Source_Object_ID"))).distinct().rdd.flatMap(lambda x: x).collect()


# check di table in source object structure
def check_di_table(table_name, df):
    count = df.filter(lower(col("Source_Object_ID")) == table_name).distinct().count()
    return True if count != 0 else False


# get the number of columns for column count check the di table
def get_column_counts(df, di_table_name):
    return df.filter(col("Source_Object_ID") == di_table_name).distinct().count()


# get the non-string columns for the DI table for data type check from source object structure table
def get_non_string_columns(df, table_name):
    rows = df.filter(col("Source_Object_ID") == table_name).filter(
        lower(trim(col("data_type"))).isin('string', 'char', 'varchar', 'nvarchar') == False).select("source_object_attribute_name",
                                "data_type").distinct().rdd.map(lambda x: list(x)).collect()
    format_rows = {}
    for row in rows:
        column_name = row[0].strip().replace("-", "_").lower()
        if column_name == "m_timestamp":
            continue
        column_type = row[1].strip().lower()
        if column_type in ["numeric", "decimal", "integer", "american", "tinyint", "bigint", "real", "float", "number"]:
            column_type = "int"
        elif column_type in ["datetime", "timestamp", "date"]:
            column_type = "date"
        if column_type in format_rows:
            format_rows[column_type].append(column_name)
        else:
            format_rows[column_type] = [column_name]
    unique_cols = {}
    for key, cols in format_rows.items():
        unique_cols[key] = set(cols)
    return unique_cols


# get the length ,scale of the columns for numeric check validation
def get_numeric_columns(df, table_name):
    format_rows = {}
    rows = df.filter(col("Source_Object_ID") == table_name).filter(lower(col("data_type")).isin('string', 'char', 'varchar') == False).filter(lower(col("scale").isin('', 'N/A')) == False).select("Source_Object_Attribute_Name", "Data_Type", "Scale").distinct().rdd.map(lambda x: list(x)).collect()
    for row in rows:
        column_name = row[0].strip().replace("-", "_").lower()
        column_type = row[1].strip().lower()
        if column_type in ["numeric", "decimal", "integer", "american", "tinyint", "bigint", "real", "float", "number"]:
            try:
                scale = int(float(row[2].strip()))
            except ValueError as e:
                print e
                print "incorrect scale value for the column {0} ".format(column_name)
            else:
                format_rows[column_name] = scale
    return [(k, v) for k, v in format_rows.iteritems()]


# get the mandatory columns for null check from source object structure table
def get_mandatory_columns(df, table_name):
    mandatory_cols = df.filter(col("Source_Object_ID") == table_name).filter(lower(col("Is_Mandatory")) == "y").select(lower(col("Source_Object_Attribute_Name"))).distinct().rdd.map(lambda x: x[0]).collect()
    mandatory_cols = [column.replace("-", "_").strip() for column in mandatory_cols if column.lower() != "m_timestamp"]
    return set(mandatory_cols)


# get the primary columns for duplicate check from source object structure table
def get_primary_key_columns(df, table_name):
    primary_cols =  df.filter(col("Source_Object_ID") == table_name).filter(lower(col("Source_Unique_Key")) == "y").select(lower(col(
        "Source_Object_Attribute_Name"))).distinct().rdd.map(lambda x: x[0]).collect()
    primary_cols = [column.replace("-", "_") for column in primary_cols if column.lower() != "m_timestamp"]
    return set(primary_cols)


# get the date and time stamp patterns from source object structure table
def get_time_stamp_pattern(df, table_name):
    format_rows = []
    rows = dict(df.filter(col("Source_Object_ID") == table_name).where(lower(col("Data_Pattern")).like("%yyyy%") | lower(col("Data_Pattern")).like("%hh%")).select(lower(col(
        "Source_Object_Attribute_Name")), lower(col("Data_Pattern"))).distinct().rdd.map(lambda x: (x[0], x[1])).collect())
    for key, value in rows.items():
        col_name = key.strip().replace("-", "_").lower()
        data_pattern = value.strip()
        format_rows.append((col_name, data_pattern))
    return set(format_rows)


# get the column order from source object structure table
def get_column_order(df, table_name):
    try:
        column_order = df.filter(lower(col("Source_Object_ID")) == table_name).withColumn("Column_Order", col("Column_Order").cast('int')).orderBy(
            "Column_Order").select("Source_Object_Attribute_Name").distinct().rdd.map(lambda x: x[0]).collect()
    except Exception as e:
        print e
        return None
    else:
        return column_order


# create the log file
def create_log_file(current_dir, log_date, source_name):

    log_file = "data_quality_metrics_{0}.log".format(source_name)

    log_file_loc = os.path.join(current_dir, "..", "logs", log_date)

    if not os.path.exists(log_file_loc):
        os.makedirs(log_file_loc)

    log_file_path = os.path.join(log_file_loc, log_file)

    log = open(log_file_path, 'a')
    sys.stdout = log


def check_cob_date(sparksession, database_table_name):
    df = sparksession.table(database_table_name)
    return True if "cob_date" in df.columns else False


def get_corresponding_table(df, table_name, hive_table_name):
    if check_di_table(table_name, df):
        return table_name
    elif check_di_table(hive_table_name, df):
        return hive_table_name
    else:
        return None


def run_time(start_time):
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    ends = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    time_taken = relativedelta(ends, start)
    return time_taken


def main():
    job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_date = datetime.today().strftime("%Y-%m-%d")

    properties_file_path = os.path.join(current_dir, "..", "config", "datarecon.config")
    props = read_properties_file(properties_file_path)

    hive_database_name = props["database_name"]
    metrics_table_name = props["mariadb_data_metrics_table_name"]
    spark_conf = create_spark_conf("data quality metrics")

    spark_session = create_spark_session(spark_conf)
    # fetch all the hive table names from corresponding db
    hive_tables = fetch_hive_tables(spark_session, hive_database_name)

    # for historical run
    if len(sys.argv) == 4:
        source_name = sys.argv[1]
        hive_table_name = sys.argv[2]
        table_name = sys.argv[3]
        cob_date = sys.argv[4]
        di_tables_info = (source_name, table_name, hive_table_name, cob_date, None)
    # for daily run
    else:
        di_tables_info = sys.argv[1]
        di_tables_info = di_tables_info.replace('((', '(').replace('))', ')')
        if ',)' in di_tables_info:
            di_tables_info = eval(di_tables_info.replace('((', '(').replace(',)', ')').replace('))', ')'))
            di_tables_info = (di_tables_info, )
        else:
            di_tables_info = eval(di_tables_info.replace('((', '(').replace('))', ')'))
        if di_tables_info:
            source_name = di_tables_info[0][0]
        else:
            print "no tables are there"
            print "--------------------------- COMPLETED ---------------------------  "
            sys.exit(0)

        create_log_file(current_dir, log_date, source_name)

        sr_obj_str_table_name = "source_object_structure_" + source_name

        print "source object structure table name is {0}".format(sr_obj_str_table_name)

        source_obj_df = get_source_object_structure_table(spark_session, props, sr_obj_str_table_name)
        source_obj_df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    print "--------------------------- STARTED ---------------------------  "
    print "Started at {0}".format(job_start_time)
    print "Total number of tables are {0} for the source {1}".format(source_name, len(di_tables_info))

    stats = []
    i = 0
    # run all the applicable metrics for all the tables in a source
    for source_name, table_name, hive_table_name, cob_date, count in di_tables_info:
        table_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        i += 1
        print "***************************************************************************"
        print "Running for Source name {0} table name {1} with number {2} cob date is {3}".format(source_name, i, table_name, cob_date)
        # check whether the table exists in hive or not
        if check_hive_table(hive_tables, hive_table_name):
            database_table_name = "{0}.{1}".format(hive_database_name, hive_table_name)
            if check_cob_date(spark_session, database_table_name):
                df = fetch_hive_table(spark_session, database_table_name, cob_date)
                df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
            else:
                print "cob date column is not there in the table {0}".format(hive_table_name)
                continue
        else:
            print "couldn't able to find the table {0} in hive ".format(hive_table_name)
            continue
        print "--------------------------------------------------------------------------"

        # Running the metric empty table . It returns status as success if the table has records else error
        print "Running the metric {0}".format("empty table check")
        empty_check_start_time = time.time()
        if not count:
            empty_record_status, empty_record_remarks = empty_records_check(df)
        else:
            empty_record_status = "error" if count == 0 else "success"
            empty_record_remarks = "Total no: of records are {0}".format(count)
        empty_check_end_time = time.time()
        empty_check_run_time = int(empty_check_end_time - empty_check_start_time)
        print "The time taken to run for empty check for the table %s is %d" % (
            table_name, empty_check_run_time)
        print "status is {0} remarks is {1}".format(empty_record_status, empty_record_remarks)
        stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "empty table check",
                      empty_record_status, empty_record_remarks))
        stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "record count check",
                      "success", "TDAP count matches with the source count"))

        # if the table is empty not running any further metrics
        if empty_record_status == "error":  # error means a empty file/table no need to run any other metric
            print "{0} table has empty records for cob date {1} so not running any other metric".format(hive_table_name,cob_date)
            df.unpersist()
            table_time_taken = run_time(table_start_time)
            print "The time taken to run for the table %s is %d hours %d minutes %d seconds" % (
                table_name, table_time_taken.hours, table_time_taken.minutes, table_time_taken.seconds)
            continue

        print "--------------------------------------------------------------------------"
        # Running the metric schema check . Checking whether the columns are in the same order as in metadata and also
        # matches the column names in metadata is same as that of hive table column names
        print "Running the metric {0}".format("schema check")
        schema_check_start_time = time.time()
        metric_table_name = get_corresponding_table(source_obj_df, table_name, hive_table_name)
        if metric_table_name:
            column_order = get_column_order(source_obj_df, metric_table_name)
            if column_order:
                schema_check_status, schema_check_remarks = schema_check(df, column_order)
                print "status is {0} remarks is {1}".format(schema_check_status, schema_check_remarks)
                stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "schema check",
                              schema_check_status, schema_check_remarks))
            else:
                print "couldn't able to get the column order for the table {0} in {1} table".format(table_name,
                                                                                                    sr_obj_str_table_name)
        else:
            print "couldn't able to find the entries for the table {0} in {1} table".format(table_name,
                                                                                            sr_obj_str_table_name)
            df.unpersist()
            table_time_taken = run_time(table_start_time)
            print "The time taken to run for the table %s is %d hours %d minutes %d seconds" % (
                table_name, table_time_taken.hours, table_time_taken.minutes, table_time_taken.seconds)
            continue
        print "--------------------------------------------------------------------------"
        schema_check_end_time = time.time()
        schema_check_run_time = int(schema_check_end_time - schema_check_start_time)
        print "The time taken to run for schema check for  the table %s is %d" % (
            table_name, schema_check_run_time)

        # Running the metric data type check. Checking for the types int and date.
        print "Running the metric {0}".format("data type check")
        data_type_start_time = time.time()
        schema = get_non_string_columns(source_obj_df, metric_table_name)
        if schema:
            data_check_status, data_check_remarks = data_type_check(df, schema)
            stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "data type check", data_check_status, data_check_remarks))
        else:
            print "couldn't able to find any non string columns for the table {0} in table {1}".format(table_name,
                                                                                                       sr_obj_str_table_name)
        print "--------------------------------------------------------------------------"
        data_type_end_time = time.time()
        data_type_run_time = int(data_type_end_time - data_type_start_time)
        print "The time taken to run for data type check for  the table %s is %d" % (
            table_name, data_type_run_time)

        # Running the metric numeric check. Checking the precision for decimal values
        print "Running the metric {0}".format("numeric check")
        numeric_type_start_time = time.time()
        col_schema = get_numeric_columns(source_obj_df, metric_table_name)
        if col_schema:
            numeric_check_status, numeric_check_remarks = numeric_check(df, col_schema)
            stats.append((log_date, source_name, table_name, hive_table_name,  cob_date, "numeric check", numeric_check_status, numeric_check_remarks))
        else:
            print "couldn't able to find any decimal columns for the table {0} in table {1}".format(table_name,sr_obj_str_table_name)

        numeric_type_end_time = time.time()
        numeric_type_run_time = int(numeric_type_end_time - numeric_type_start_time)
        print "The time taken to run for numeric check for  the table %s is %d" % (
            table_name, numeric_type_run_time)

        print "--------------------------------------------------------------------------"
        # Running the metric null check. Checking whether the column is empty for mandatory columns
        print "Running the metric {0}".format("null check")
        null_check_start_time = time.time()
        mandatory_columns = get_mandatory_columns(source_obj_df, metric_table_name)
        if mandatory_columns:
            null_check_status, null_check_remarks = null_check(df, mandatory_columns)
            stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "null check", null_check_status,
                          null_check_remarks))
        else:
            print "couldn't able to find any mandatory columns for the table {0} in table {1}".format(table_name,
                                                                                                       sr_obj_str_table_name)
        null_check_end_time = time.time()
        null_check_run_time = int(null_check_end_time - null_check_start_time)
        print "The time taken to run for null check for  the table %s is %d" % (
            table_name, null_check_run_time)

        print "--------------------------------------------------------------------------"
        # Running the metric duplicate check. Checking whether there duplicate value for primary columns
        print "Running the metric {0}".format("duplicate check")
        duplicate_check_start_time = time.time()

        primary_columns = get_primary_key_columns(source_obj_df, metric_table_name)
        if primary_columns:
            duplicate_check_status, duplicate_check_remarks = duplicate_check(df, primary_columns)
            stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "duplicate check", duplicate_check_status,
                          duplicate_check_remarks))
        else:
            print "couldn't able to find any primary columns for the table {0} in table {1}".format(table_name,
                                                                                                      sr_obj_str_table_name)
        duplicate_check_end_time = time.time()
        duplicate_check_run_time = int(duplicate_check_end_time - duplicate_check_start_time)
        print "The time taken to run duplicate check for the table %s is %d" % (
            table_name, duplicate_check_run_time)
        
        print "--------------------------------------------------------------------------"
        # Running the metric date timestamp check. Checking whether the date column matches with the pattern
        # mentioned in metadata.
        timestamp_start_time = time.time()
        print "Running the metric {0}".format("date/timestamp check")
        cols = get_time_stamp_pattern(source_obj_df, metric_table_name)
        if cols:
            date_time_status, date_time_remarks = date_timestamp_check(df, cols)
            stats.append((log_date, source_name, table_name, hive_table_name, cob_date, "date/timestamp check", date_time_status,
                          date_time_remarks))
        else:
            print "couldn't able to find any date timestamp patterns for the table {0} in table {1}".format(table_name,
                                                                                                    sr_obj_str_table_name)
        timestamp_end_time = time.time()
        timestamp_run_time = int(timestamp_end_time - timestamp_start_time)
        print "The time taken to run date/timestamp check for the table %s is %d" % (
            table_name, timestamp_run_time)

        df.unpersist()

        table_time_taken = run_time(table_start_time)
        print "The time taken to run for the table %s is %d hours %d minutes %d seconds" % (
            table_name, table_time_taken.hours, table_time_taken.minutes, table_time_taken.seconds)

        if len(stats) % 50 == 0:
            insert_metrics_mariadb(spark_session, stats, props, metrics_table_name)
            stats = []

    insert_metrics_mariadb(spark_session, stats, props, metrics_table_name)

    close_spark_session(spark_session)

    time_taken = run_time(job_start_time)

    print "The time taken to run is %d hours %d minutes %d seconds" % (
        time_taken.hours, time_taken.minutes, time_taken.seconds)

    print "--------------------------- COMPLETED ---------------------------  "


if __name__ == "__main__":
    main()
