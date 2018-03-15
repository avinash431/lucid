package com.avinash.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemCat {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String uri = args[0];
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    InputStream in = null;
	    try {
	      in = fs.open(new Path(uri));
	      IOUtils.copyLarge(in, System.out);
	    } finally {
	      IOUtils.closeQuietly(in);
	    }

	
		

	}

}
