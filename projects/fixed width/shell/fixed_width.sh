#!/bin/sh

scripts_dir=$(cd `dirname $0` && pwd)

hive_script_dir=$(cd ${scripts_dir} && cd ../hql && pwd)
log_dir=$(cd ${scripts_dir} && cd ../logs && pwd)

hive_script_file=$hive_script_dir/insert_branch_details.sql
fixed_width_jar_loc=""
file_input_loc="/user/lucid/inputfiles"
file_name="employees.txt"
database_name="lucid"
table_name="employess_dept"
BEELINE_COMMAND="jdbc:hive2://"

hdfs_input_path=/user/lucid/hadoop/inputfiles
hdfs_output_path=/user/lucid/hadoop/outputfiles
schema_file=${file_input_loc}/metadata.txt

#get the staging table and rep table location
fetch_table_loc(){

	table_loc=`beeline -u $BEELINE_COMMAND --silent=true -e "describe formatted $database_name.$staging_table_name" | grep Location | cut -d '|' -f3 `
	table_loc="$(echo -e "${table_loc}" | tr -d '[:space:]')"
		
	echo -e "Staging table location is "$table_loc
}


#check the input file_input_file

if [[ ! -f ${file_input_loc}/${file_name} ]]
then
	echo -e "file doesn't exist"
	exit 1
fi

hdfs dfs -put ${file_input_loc}/${file_name} $hdfs_file_input_path

yarn jar ${fixed_width_jar_loc}/fixedwidth.jar -files $schema_file ${hdfs_file_input_path}/$file ${hdfs_file_output_path}

if [[ $? -eq 0 ]]
		then
			echo -e "Map Reduce job completed succesfully and removing the success/part empty file"			
			hdfs dfs -rm -skipTrash ${hdfs_file_output_path}/_SUCCESS			
			echo -e "Moving the output files to hive staging table"			
			#check whether the cob date folder is already created in the staging table
			if [[ $? -eq 0 ]]
			then
				hdfs dfs -mv ${hdfs_file_output_path}/* ${table_loc}/			
			fi
		hdfs dfs -rm -skipTrash -r ${hdfs_file_input_path}/$file
		hdfs dfs -rm -skipTrash -r $hdfs_file_output_path
	else
		echo -e "Fixed width script failed and deleting the files in the staging table folder"
		hdfs dfs -rm -skipTrash -r ${hdfs_file_input_path}/$file
		hdfs dfs -rm -skipTrash -r -f $table_loc/*
		hdfs dfs -rm -skipTrash -r $hdfs_file_output_path
		exit 1
	fi
