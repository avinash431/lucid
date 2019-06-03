import subprocess
import os
import sys

def load_properties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


if __name__ == "__main__" : 	
    source_files_list = {}
    current_dir= os.getcwd()
    config_file = current_dir + "/config/" +"datarecon.config"
    jar_file_path = current_dir + "/jars/"
    maria_jar_file_path = jar_file_path + "mariadb-java-client-2.3.0.jar"
    spark_jar_file_path = jar_file_path + "maria-datarecon.jar"
    source_file_path = current_dir + "/files/"
    log_path = current_dir + "/logs/"
    log_file = log_path + "datarecon.log"

    list_sources = os.listdir(source_file_path)
    sources_list = [i.split(".")[0] for i in list_sources]
    config = load_properties(config_file)
    path = config['file_path']
    
    for source,file_name in zip(sources_list,list_sources):
        print source,file_name
        files_list_path=source_file_path+file_name
        f = open(files_list_path,'r')
        files_list = [x.strip() for x in f.readlines()]
        source_files_list[source] = files_list

    cmd = 'spark-submit --class com.dbs.tdap.itt.DataRecon --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.executor.cores=4 --conf spark.executor.memory=4G --jars {0} --driver-class-path {0} --master yarn {1} "{2}" {3} &> {4} '.format(maria_jar_file_path,spark_jar_file_path,source_files_list,config_file,log_file)
    print cmd
    subprocess.Popen(cmd,stdout=f,shell=True)
