CREATE EXTERNAL TABLE user_preferences(
  datetime string ,
  ackflag string ,
  canviewlimitorders string ,
  defaultaccount string ,
  fullname string ,
  lang string ,
  outstandingcontractalertemail string ,
  outstandingcontractalertsms string ,
  regionname string ,
  userexternalid string ,
  usertype string ,
  username string ,
  currencies string )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES (
 'skip.header.line.count'='1',
 );


CREATE EXTERNAL TABLE RET_DOL_USER_CURRENCY_PREFERENCE_SG(
  date_time string,
  cin string,
  username string,
  currency_pair string)
STORED AS PARQUET
