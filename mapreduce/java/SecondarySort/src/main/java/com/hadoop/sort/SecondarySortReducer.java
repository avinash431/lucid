package com.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<CustomKey, Text, NullWritable, Text> {
	
	@Override
	protected void reduce(CustomKey key, Iterable<Text> values,
			Reducer<CustomKey, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		for (Text text : values) {
			context.write(NullWritable.get(), text);
			
		}
	}

}
