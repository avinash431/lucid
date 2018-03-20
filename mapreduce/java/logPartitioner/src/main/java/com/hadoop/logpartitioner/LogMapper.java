package com.hadoop.logpartitioner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class LogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> multiouts = null;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.multiouts = new MultipleOutputs<>(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String fields[] = line.split("\\s+");
		String date = fields[0];
		multiouts.write(NullWritable.get(), value, generateKey(date));
	}

	private String generateKey(String date) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		String[] date_split = date.split("/");
		String month = date_split[0];
		String day = date_split[1];
		String year = date_split[2];
		sb.append("year=");
		sb.append(year);
		sb.append("/");
		sb.append("month=");
		sb.append(month);
		sb.append("/");
		sb.append("day=");
		sb.append(day);
		sb.append("/");
				
		return sb.toString();
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multiouts.close();
	}

}