package com.avinash.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MoveFiles extends Configured {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//MoveFiles mv = new MoveFiles();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(args[0]);
		Path dest = new Path(args[1]);
		String modified_output_path = output.toString() +"/*";
		System.out.println("Files to be moved in the location "+modified_output_path.toString());
		Path modified_path = new Path(modified_output_path);
		fs.rename(modified_path, dest);

	}
}
