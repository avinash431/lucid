package com.hadoop.bucketing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BucketMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		
		if(line.contains("sex")) {
			return;
		}
		else {
			String[ ] record = line.split(",");
			String continent="";
			if(record.length == 17) {
				continent = record[7];
				if(! continent.equals("") && ! continent.equals("Unknown")) {
				context.write(new Text(continent), value);
				}
			}
		}
	
		
	}

	public static void main(String[] args) {
		String line = "730866,Asa,Male,Unknown,Other,,Unknown,Unknown,,,Po"
				+ "litician,Government,Institutions,31,282386,9109,12.3371";
		
		String[ ] record = line.split(",");
		String continent="";
		if(record.length == 17) {
			continent = record[7];
			System.out.println(continent.equals("Unknown"));
			System.out.println(continent.equals(""));
			if(! continent.equals("") && ! continent.equals("Unknown")) {
				System.out.println(continent);
			}
			
			String keys = "Asia,Africa,Europe,North America,Oceania,South America,continent";
			for(String key:keys.split(",")) {
			//System.out.println(""+key.hashCode());
			System.out.println(key +" " + key.charAt(2)*127 %7);
			}
			
			}
	}
}
