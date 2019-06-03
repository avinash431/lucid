insert into table lucid.branch_details 
select 
Institution_Name,
CAST(Main_Office as INT),
Branch_Name,
CAST(Branch_Number as INT),
CAST(from_unixtime(unix_timestamp(Established_Date ,'DD/MM/YYYY'), 'YYYY-MM-DD') as DATE),
CAST(from_unixtime(unix_timestamp(Acquired_Date ,'DD/MM/YYYY'), 'YYYY-MM-DD') as DATE),
Street_Address,
City,
County,
State,
Zipcode 
from lucid.branch_details_stg;
