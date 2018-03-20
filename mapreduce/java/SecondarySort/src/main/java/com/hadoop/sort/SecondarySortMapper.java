package com.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CustomKey, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		
		if(line.contains("sex")) {
			return;
		}
		else {
			String[ ] record = line.split(",");
			String continent="";
			String country="";
			CustomKey obj;
			if(record.length == 17) {
				continent = record[7];
				country  = record[6];
				if((! continent.equals("") && ! continent.equals("Unknown")) && !country.equals("")) {
					obj = new CustomKey();
					obj.setContinent(new Text(continent));
					obj.setCountry(new Text(country));
					context.write(obj, value);
				}
				
				
			}
	}
 }

}
