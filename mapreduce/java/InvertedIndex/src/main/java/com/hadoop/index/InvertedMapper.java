package com.hadoop.index;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String filename;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 this.filename= ((FileSplit)context.getInputSplit()).getPath().getName();
		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String[ ] words = line.split(" ");
		String word="";
		for (int i = 0; i < words.length; i++) {
			word = words[i];
			word = word.replaceAll("[^a-zA-Z ]", "").toLowerCase();
			context.write(new Text(word), new Text(this.filename));
		}
	}
	
	

}
