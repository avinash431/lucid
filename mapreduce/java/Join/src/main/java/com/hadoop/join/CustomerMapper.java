package com.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		String record = value.toString();
		String customers[] = record.split(",");
		StringBuilder customerDetails = new StringBuilder();
		customerDetails.append("c,");
		String customerName = "";
		for (int i = 0; i < customers.length; i++) {
			if(i == 1){
				customerName=customers[i];
			}
			else{
				customerDetails.append(customers[i]);
				customerDetails.append(",");	
			}
			
		}
		
		customerDetails.deleteCharAt(customerDetails.length()-1);
		context.write(new Text(customerName), new Text(customerDetails.toString()));
		
		
	}

}
