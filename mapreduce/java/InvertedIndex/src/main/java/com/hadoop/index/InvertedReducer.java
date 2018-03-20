package com.hadoop.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		for (Text text : value) {
			sb.append(text.toString());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		context.write(key, new Text(sb.toString()));
	}
	

}
