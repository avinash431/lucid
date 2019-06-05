use lucid;
insert overwrite table user_partitions
select
datetime AS Date_Time ,
defaultAccount AS CIN ,
username AS Username ,
currencies AS Currency_Pair
from user_partitions_stg;
