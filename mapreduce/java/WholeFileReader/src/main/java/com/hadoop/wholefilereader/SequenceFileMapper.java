package com.hadoop.wholefilereader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
	
	private Text filenameKey; 
	
	@Override
	protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		InputSplit inputSplit =context.getInputSplit();
		Path path = ((FileSplit)inputSplit).getPath();
		this.filenameKey = new Text(path.getName());
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(filenameKey, value);
	}

}

