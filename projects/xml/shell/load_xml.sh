#/bin/sh


scripts_dir=$(cd `dirname $0` && pwd)

hive_script_dir=$(cd ${scripts_dir} && cd ../hql && pwd)
log_dir=$(cd ${scripts_dir} && cd ../logs && pwd)

hive_script_file=$hive_script_dir/insert_tables.sql

file_input_loc="/user/lucid/inputfiles"
file_name="customer_books.xml"
database_name="lucid"
staging_table_name="xml_temp"

BEELINE_COMMAND="jdbc:hive2://"

if [[ ! -d $log_dir ]]
then
    mkdir -p $logdir
fi

exec &>>${log_dir}/bank_flat_file.log

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

hdfs dfs -put ${file_input_loc}/${file_name} $staging_table_loc

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
