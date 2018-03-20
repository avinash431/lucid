package com.hadoop.fixedwidth;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class FixedWidthMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private ArrayList<String> ar = new ArrayList<String>();
	private static Logger log = Logger.getLogger(FixedWidthMapper.class);
	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path[] uri =Job.getInstance(conf).getLocalCacheFiles();
		log.info("URI isssssssssssssssss "+uri);
		for(Path path: uri){
			readFile(path.getName());
		}
		log.info("List issssssssssssssssss "+ar);
	}

	private void readFile(String lookup) {
		
		BufferedReader br =null;
		try {
			br  = new BufferedReader(new FileReader(lookup));
			String line =null ;
			while((line = br.readLine())!=null ){
				log.info("fileeeeeeeee "+line);
				ar.add(line);		
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	  String line = value.toString();
	  StringBuilder sb = new StringBuilder();
	  context.getCounter(ErrorRecord.mismatched).setValue(0);
	  log.info("lengthhhhhhhhhhhhhh "+line.length());
	  if(line.length() != 18){
		  context.getCounter(ErrorRecord.mismatched).increment(1);
	  }
	  else{
		  log.info("line isssss "+line);
		  for (Iterator<String> iterator = ar.iterator(); iterator.hasNext();) {
			  String pos = iterator.next();
			  int pos1 =Integer.parseInt(pos.split("-")[0]);
			  int pos2 =Integer.parseInt(pos.split("-")[1]);
			  log.info("substr "+line.substring(pos1, pos2));
			 sb.append(line.substring(pos1, pos2));
			 sb.append(",");	
		}
		  sb.deleteCharAt(sb.length()-1);
	  }
	  if(context.getCounter(ErrorRecord.mismatched).getValue() ==0){
		  log.info("--------------------------------- "+sb);
		  context.write(new Text(sb.toString()), NullWritable.get());
	  }
	  else{
		  System.exit(1);
	  }
	}

}
