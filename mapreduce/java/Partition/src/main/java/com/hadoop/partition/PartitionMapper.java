package com.hadoop.partition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PartitionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> multiouts;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multiouts = new MultipleOutputs<NullWritable,Text>(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		if(line.contains("date")) {
			return;
		}
		else {
		String[ ] records = line.split(",");
		String date="";
		if(records.length == 7) {
			date=records[0];
			if(date != null && !date.equals("")) {
				multiouts.write(NullWritable.get(), value, "date="+date+"/");
			}
		}
		}
		
	}
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multiouts.close();
	}
	
	


}
