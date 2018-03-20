package com.hadoop.bucketing;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BucketReducer extends Reducer<Text, Text, NullWritable, Text> {

	

	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text text : values) {
			context.write(NullWritable.get(), text);
			
		}
	}

}
