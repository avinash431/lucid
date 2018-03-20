package com.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String record = value.toString();
		String orders[] = record.split(",");
		StringBuilder orderDetails = new StringBuilder();
		orderDetails.append("o,");
		String customerName = "";
		for (int i = 0; i < orders.length; i++) {
			if(i == 2){
				customerName=orders[i];
			}
			else{
				orderDetails.append(orders[i]);
				orderDetails.append(",");	
			}
			
		}
		
		orderDetails.deleteCharAt(orderDetails.length()-1);
		context.write(new Text(customerName), new Text(orderDetails.toString()));
	}

}