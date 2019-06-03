create database if not exists lucid;
use lucid;

create table branch_details_stg(
Institution_Name string,
Main_Office	string,
Branch_Name	string,
Branch_Number string,
Established_Date string,
Acquired_Date string,
Street_Address string,
City string,
County string,
State string,
Zipcode string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
'quoteChar'='\"',
'separatorChar'=',')
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
'skip.header.line.count'='1'
)
LOCATION
'/user/lucid/hive/branch_details_stg';

create external table branch_details(
Institution_Name string,
Main_Office	int,
Branch_Name	string,
Branch_Number int,
Established_Date date,
Acquired_Date date,
Street_Address string,
City string,
County string,
State string,
Zipcode int)
STORED AS PARQUET
LOCATION
'/user/lucid/hive/branch_details' ;
