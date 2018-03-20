package com.hadoop.mapjoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LeftOuterJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	
private HashMap<String, String> inputdata = new HashMap<String,String>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		URI[] uri = context.getCacheFiles();
		
		Path customerpath = new Path(uri[0]);
		readFile(customerpath.getName(),context);
		
		
	}
	
	private void  readFile(String file,Context context){	
			
			BufferedReader br =null;
			try{
				br = new BufferedReader(new FileReader(file));
				String line =null ;
				StringBuilder customerDetails = null;
				String customerName = null;
				while((line = br.readLine())!=null ){
					context.getCounter(MapEnum.count).increment(1);
					customerDetails = new StringBuilder();
					String customers[] = line.split(",");
					for (int i = 0; i < customers.length; i++) {
						if(i == 1){
							customerName=customers[i];
						}
						else{
							customerDetails.append(customers[i]);
							customerDetails.append(",");	
						}
						
					}
					inputdata.put(customerName, customerDetails.toString());
				}				
				
			}catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		

		}
	
	@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		String record = value.toString();
		String orders[] = record.split(",");
		StringBuilder orderDetails = new StringBuilder();
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
		
		String customerdata = inputdata.get(customerName);
		if(customerdata!=null){
			context.write(new Text(customerName), new Text(customerdata.toString()+","+orderDetails.toString()));			
		}
		
		else{
			context.write(new Text(customerName),new Text(customerdata));
		}
		}

}