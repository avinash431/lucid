package com.hadoop.compressioncodec;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FileCompressor {
	
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Class<?> compressionclass = Class.forName(args[1]);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(compressionclass, conf);
		
		InputStream in = fs.open(new Path(args[0]));
		OutputStream out = fs.create(new Path(args[0]+codec.getDefaultExtension()));
		OutputStream cos = codec.createOutputStream(out);
		IOUtils.copyBytes(in, cos, conf);
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		
	}

}
