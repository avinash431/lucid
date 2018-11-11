-- partiiton table

create table employees(id int, name string,city string)
partitioned by (country string)
row format delimited
fields terminated by ',';

--static partition

insert into table employees partition(country='IND') values (1, 'avinash', 'hyd');

load data local inpath '/home/cloudera/hive-data/uk.csv' into table employees partition(country = 'uk');
load data local inpath '/home/cloudera/hive-data/us.csv' into table employees partition(country = 'us');
load data local inpath '/home/cloudera/hive-data/aus.csv' into table employees partition(country = 'aus');

-- check the hdfs path of the table to see the partitioned directories and the files

-- dynamic partition

 create table employee_stg ( id int, name string, city string,country string)
 row format delimited 
 fields terminated by ',';
 
 create table employee_partition(id int,name string,city string)
 partitioned by (country string);
 
 load data local inpath '/home/cloudera/hive-data/countries.csv' into table employee_stg;
 
 set hive.exec.dynamic.partition.mode=nonstrict
 
 insert into table employee_partition partition(country) select * from employee_stg;
 
 -- static partition with msck
 
 hdfs dfs -mkdir -p /user/training/hdfs/hive/countries/country=uk
 hdfs dfs -mkdir -p /user/training/hdfs/hive/countries/country=us
 hdfs dfs -mkdir -p /user/training/hdfs/hive/countries/country=aus
 
 hdfs dfs -put uk.csv /user/training/hdfs/hive/countries/country=uk/
 hdfs dfs -put us.csv /user/training/hdfs/hive/countries/country=us/
 hdfs dfs -put aus.csv /user/training/hdfs/hive/countries/country=aus/
 
 create table employee_msck (id int,name string,city string)
 partitioned by (country string)
 row format delimited 
 fields terminated by ''
 LOCATION '/user/training/hdfs/hive/countries' ;
 
 select * from employee_msck;
 
 msck repair table employee_msck;
 
 
 
