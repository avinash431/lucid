package com.hadoop.wordcount.Hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count = 0;
		for (LongWritable val : values) {
			count+=val.get();
			
		}
		context.write(key, new LongWritable(count));
	}

}
