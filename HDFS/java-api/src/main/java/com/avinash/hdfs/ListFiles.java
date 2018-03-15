package com.avinash.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFiles {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		try {
			FileSystem fs = FileSystem.get(conf);
			
			Path input = new Path(args[0]);
			
			FileStatus[] filestatus = fs.listStatus(input);
			for (int i = 0; i < filestatus.length; i++) {
				System.out.println(filestatus[i].getPath().getName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
