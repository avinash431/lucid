from pyspark.sql.types import BooleanType, IntegerType
from pyspark.sql.functions import udf, col, lit, length, isnan, sum
from datetime import datetime
import time


def is_digit(val):
    if val:
        try:
            float(val)
        except ValueError as e:
            print e
            return 1
        else:
            return 0
    else:
        return 0


def is_date(val):
    if val:
        if len(val) == 8:
            date_format = '%Y%m%d'
            try:
                date_obj = datetime.strptime(val, date_format)
                print date_obj
            except Exception as e:
                print val, e
                print "Incorrect data format, should be {0}".format(date_format)
                return 1
            else:
                return 0
        elif len(val) == 10:
            if '-' in val:
                date_format = '%Y-%m-%d'
                print "format is YYYY-MM-DD"
                try:
                    date_obj = datetime.strptime(val, date_format)
                    print date_obj
                except Exception as e:
                    print val, e
                    print "Incorrect data format, should be {0}".format(date_format)
                    return 1
                else:
                    return 0
            elif '/' in val:
                date_format = '%d/%m/%Y'
                print "format is DD/MM/YYYY"
                try:
                    date_obj = datetime.strptime(val, date_format)
                    print date_obj
                except Exception as e:
                    print val, e
                    print "Incorrect data format, should be {0}".format(date_format)
                    return 1
                else:
                    return 0
        elif len(val) == 19:
            if '-' in val:
                date_format = '%Y-%m-%d %H:%M:%S'
                print "format is YYYY-MM-DD HH:MM:SS"
                try:
                    date_obj = datetime.strptime(val, date_format)
                    print date_obj
                except Exception as e:
                    print val, e
                    print "Incorrect data format, should be {0}".format(date_format)
                    return 1
                else:
                    return 0
            elif '/' in val:
                date_format = '%d/%m/%Y %H:%M:%S'
                print "format is DD/MM/YYYY HH:MM:SS"
                try:
                    date_obj = datetime.strptime(val, date_format)
                    print date_obj
                except Exception as e:
                    print val, e
                    print "Incorrect data format, should be {0}".format(date_format)
                    return 1
                else:
                    return 0
        elif len(val) == 13:
            print "date is in epoch format"
            try:
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(val)))
            except Exception as e:
                print val, e
                print "Date is incorrect epoch format"
                return 1
            else:
                return 0
    else:
        return 0


def scale_correct(num, scale):
    if not num:
        return 0
    if scale == 0:
        return 1 if '.' in num else 0
    else:
        if '.' not in num:
            return 1
        else:
            scale_value = num.split('.')[1]
            if scale == len(scale_value):
                return 0
            return 1


def is_date_time(val, date_format):
    val = val.strip()
    if len(val) > 0:
        try:
            datetime.strptime(val, date_format)
        except ValueError:
            print("Incorrect data format, should be {0}".format(date_format))
            return 1
        else:
            return 0
    else:
        return 0


def is_null(val):
    if val:
        try:
            return 1 if len(unicode(val).strip()) == 0 else 0
        except TypeError:
            return 0
    return 1


is_digit_udf = udf(is_digit, IntegerType())
is_date_udf = udf(is_date, IntegerType())
scale_udf = udf(scale_correct, IntegerType())
is_date_time_udf = udf(is_date_time, IntegerType())
null_udf = udf(is_null, IntegerType())


# check the total no: of records of di table. return "success" if not zero
def empty_records_check(df):
    len_df = df.count()
    remarks = "Total no: of records are {0}".format(len_df)
    status = "success" if len_df != 0 else "error"
    return status, remarks


# check the total no: of columns if di table
def column_count_check(df, column_count):
    di_columns_count = len(df.columns)
    num_partition_columns = di_columns_count - df.columns.index('cob_date')
    total_columns_count = column_count + num_partition_columns
    remarks = "Total no: of columns in di table are {0} \n Expected no: of columns are {1}".\
        format(di_columns_count, total_columns_count)
    status = "error" if di_columns_count != total_columns_count else "success"
    return status, remarks


def schema_check(df, column_order):
    df_columns = [i.lower().strip() for i in df.columns]
    cob_date_index = df_columns.index("cob_date")
    df_columns = df_columns[:cob_date_index]
    column_order = [i.replace("-", "_").replace("(", "").replace(")", "").lower().strip() for i in column_order]
    if len(df_columns) != len(column_order):
        return "error", "Total no: of columns in di table are {0} and of the metadata sheet are {1}".format(len(df_columns),len(column_order))
    flag = True
    for x, y in zip(df_columns, column_order):
        if x != y:
            flag = False
            break
    status = "success" if flag else "error"
    remarks = "All the columns are matched with the metadata" if flag else \
        "columns mismatched are {0}".format(','.join(list(set(column_order) - set(df_columns))))

    return status, remarks


# check the total no: of records is same as the source expected or not
def record_count_check(df, record_count):
    df_records_count = df.count()
    remarks = "Total no: of records in di table are {0} \n Expected no: of records are {1}". \
        format(df_records_count, record_count)
    status = "success" if df_records_count != record_count else "error"
    return status, remarks


# check the non-numeric data types of the di table with the schema from source object structure table
def data_type_check(df, schemas):
    # df_cols = [i.strip() for i in df.columns]
    int_cols = schemas.get("int")
    date_cols = schemas.get("date")
    total_counts = []
    comments = ""
    try:
        if int_cols:
            int_df = df.select(*int_cols)
        else:
            print "no numeric columns are there in the table"
        if date_cols:
            date_df = df.select(*date_cols)
        else:
            print "no date columns are there in the table"
    except Exception as e:
        print e
        print "couldn't able to fetch all the columns "
        return "error", "couldn't able to fetch all the int/date type columns"
    else:
        if int_cols:
            print "checking for columns {0}".format(','.join(int_cols))
            true_false_int_df = int_df.select(*[is_digit_udf(col(col_name)).name(col_name) for col_name in int_df.columns])
            sum_int_df = true_false_int_df.select(*[sum(col(col_name)) for col_name in true_false_int_df.columns])
            int_counts = [list(row) for row in sum_int_df.collect()][0]
            int_counts = zip(int_cols, int_counts)
            int_counts = [(a, b) for a, b in int_counts if b and b != 0]
            total_counts += int_counts
            if int_counts:
                int_columns = list(map(lambda x: x[0], int_counts))
                comments += "Columns {0} has incorrect integer values".format(''.join(int_columns))

        if date_cols:
            print "checking for columns {0}".format(','.join(date_cols))
            true_false_date_df = date_df.select(*[is_date_udf(col(col_name)).name(col_name) for col_name in date_df.columns])
            sum_date_df = true_false_date_df.select(*[sum(col(col_name)) for col_name in true_false_date_df.columns])
            date_counts = sum_date_df.rdd.map(lambda x: list(x)).collect()[0]
            date_counts = zip(date_cols, date_counts)
            date_counts = [(a, b) for a, b in date_counts if b and b != 0]
            if date_counts:
                date_columns = list(map(lambda x: x[0], date_counts))
                comments += "Columns {0} has incorrect date values".format(''.join(date_columns))

        status = "error" if total_counts else "success"
        remarks = comments if comments else "All the columns are sync with the metadata"
        return status, remarks


def numeric_check(df, col_schema):
    comments = ""
    numeric_columns = [col_name for col_name, scale in col_schema]
    print "checking for columns {0}".format(','.join(numeric_columns))
    try:
        numeric_df = df.select(*numeric_columns)
    except Exception as e:
        print e
        print "couldn't able to get all numeric columns"
        return "error", "couldn't able to fetch all the numeric type columns"
    else:
        numeric_df = numeric_df.select(*[scale_udf(col_name, lit(scale)).name(col_name) for col_name, scale in col_schema])
        sum_numeric_df = numeric_df.select(*[sum(col(col_name)) for col_name in numeric_df.columns])
        numeric_counts = [list(row) for row in sum_numeric_df.collect()][0]
        numeric_counts = zip(numeric_columns, numeric_counts)
        numeric_counts = [(a, b) for a, b in numeric_counts if b and b != 0]
        if numeric_counts:
            numeric_columns = list(map(lambda x: x[0], numeric_counts))
            comments += "Columns {0} has incorrect numeric values".format(''.join(numeric_columns))

        status = "error" if numeric_counts else "success"
        remarks = comments if numeric_counts else "All the columns are sync with the metadata"
        return status, remarks


def null_check(df, mandatory_columns):
    print "checking for columns {0}".format(','.join(mandatory_columns))
    comments = ""
    try:
        mandatory_df = df.select(*mandatory_columns)
    except Exception as e:
        print e
        print "couldn't able to find all mandatory columns"
        return "error", "couldn't able to find all mandatory columns"
    else:
        true_false_int_df = mandatory_df.select(*[null_udf(col(col_name)).name(col_name) for col_name in mandatory_df.columns])
        sum_null_df = true_false_int_df.select(*[sum(col(col_name)) for col_name in true_false_int_df.columns])
        null_counts = [list(row) for row in sum_null_df.collect()][0]
        null_counts = zip(mandatory_columns, null_counts)
        null_counts = [(a, b) for a, b in null_counts if b and b != 0]
        if null_counts:
            null_columns = list(map(lambda x: x[0], null_counts))
            comments += "Columns {0} has null values".format(''.join(null_columns))
        status = "error" if null_counts else "success"
        remarks = comments if null_counts else "All the columns are sync with the metadata"
        return status, remarks


def duplicate_check(df, primary_columns):
    print "checking for columns {0}".format(','.join(primary_columns))
    total_count = df.count()
    # if there are primary columns get their distinct count else get the distinct count on the entire rows
    if primary_columns:
        primary_df = df.select(*primary_columns)
        distinct_count = primary_df.distinct().count()
    else:
        primary_df = df.distinct()
        distinct_count = primary_df.count()

    status = "success" if total_count == distinct_count else "error"
    remarks = "Total no: of duplicate records are {0}".format(total_count - distinct_count)
    return status, remarks


def date_timestamp_check(df, columns):
    comments = ""
    list_date_cols = []
    for di_col, pattern in columns:
        pattern = pattern.strip().upper()
        if pattern == "YYYY-MM-DD":
            date_format = "%Y-%m-%d"
        elif pattern == "YYYY-MM-DD HH:MM:SS":
            date_format = "%Y-%m-%d %H:%M:%S"
        elif pattern == "YYYMMDD":
            date_format = "%Y%m%d"
        elif pattern == "HHMMSS":
            date_format = "%H%M%S"
        else:
            continue
        list_date_cols.append((di_col, date_format))

    pattern_cols = [di_col for di_col, pattern in list_date_cols]
    print "checking for columns {0}".format(','.join(pattern_cols))

    try:
        pattern_df = df.select(*pattern_cols)
    except Exception as e:
        print e
        return "error", "couldn't able to get all the date/timestamp pattern columns"
    else:
        pattern_df = pattern_df.select(*[is_date_time_udf(col_name, lit(date_format)).name(col_name) for col_name, date_format in list_date_cols])
        sum_timestamp_df = pattern_df.select(*[sum(col(col_name)) for col_name in pattern_df.columns])
        pattern_counts = [list(row) for row in sum_timestamp_df.collect()][0]
        pattern_counts = zip(pattern_cols, pattern_counts)
        pattern_counts = [(a, b) for a, b in pattern_counts if b and b != 0]
        if pattern_counts:
            pattern_columns = list(map(lambda x: x[0], pattern_counts))
            comments += "Columns {0} has incorrect date/timestamp values".format(''.join(pattern_columns))
        status = "error" if pattern_counts else "success"
        remarks = comments if pattern_counts else "All the date timestamp columns are of the expected format"
        return status, remarks
