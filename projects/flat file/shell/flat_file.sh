#/bin/sh


scripts_dir=$(cd `dirname $0` && pwd)

hive_script_dir=$(cd ${scripts_dir} && cd ../hql && pwd)
base_dir=$(cd ${scripts_dir} && cd .. && pwd)
log_dir=${base_dir}/logs
hive_script_file=$hive_script_dir/flat_file_insert_branch_details.sql

file_input_loc="/home/cloudera/projects/flat_file/input"
file_name="database.csv"
database_name="lucid"
staging_table_name="branch_details_stg"

BEELINE_COMMAND="jdbc:hive2://"

if [[ ! -d $log_dir ]]
then
    mkdir $log_dir
    touch ${log_dir}/bank_flat_file.log
fi

exec &>>${log_dir}/bank_flat_file.log


echo "log dir is " $log_dir
echo "hql dir is " $hive_script_dir

#get the staging table and rep table location
fetch_table_loc(){

	staging_table_loc=`beeline -u $BEELINE_COMMAND --silent=true -e "describe formatted $database_name.$staging_table_name" | grep Location | cut -d '|' -f3 `
	staging_table_loc="$(echo -e "${staging_table_loc}" | tr -d '[:space:]')"
		
	echo -e "Staging table location is "$staging_table_loc
}


#check the input file_input_file

if [[ ! -f ${file_input_loc}/${file_name} ]]
then
	echo -e "file doesn't exist"
	exit 1
fi

fetch_table_loc

hdfs dfs -put -f ${file_input_loc}/${file_name} $staging_table_loc

if [[ $? -ne 0 ]]
then
	echo -e "Failed while uploading the file to staging table"
	exit 1
fi

beeline -u $BEELINE_COMMAND -f $hive_script_file

if [[ $? -eq 0 ]]
then
	echo -e "Ran the script succesfully"
	exit 0
else
	echo -e "Hive script failed"
	exit 1
fi



