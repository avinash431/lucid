package com.hadoop.fixedwidth;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FixedWidthMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private ArrayList<String> ar = new ArrayList<String>();
	
	private int fixedWidthlength;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//Configuration conf = context.getConfiguration();
		//Path[] uri = Job.getInstance(conf).getLocalCacheFiles();
		/*for (Path path : uri) {
		readFile(path.getName());
	}*/
		
		URI[] cacheFiles = context.getCacheFiles();
		for (int i = 0; i < cacheFiles.length; i++) {
		 Path cacheFilePath = new Path(cacheFiles[i]);
		 readFile(cacheFilePath.getName());
		}
		
		this.fixedWidthlength = context.getConfiguration().getInt("fixedwidthlenght", 0);
	}

	private void readFile(String lookup) {

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(lookup));
			String line = null;
			while ((line = br.readLine()) != null) {
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
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		StringBuilder sb = new StringBuilder();
		
		context.getCounter(ErrorRecord.mismatched).setValue(0);
		
		if (line.length() != this.fixedWidthlength) {
			context.getCounter(ErrorRecord.mismatched).increment(1);
		} else {
			for (Iterator<String> iterator = ar.iterator(); iterator.hasNext();) {
				String pos = iterator.next();
				int pos1 = Integer.parseInt(pos.split("-")[0]);
				int pos2 = Integer.parseInt(pos.split("-")[1]);
				sb.append(line.substring(pos1, pos2));
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
		}
		if (context.getCounter(ErrorRecord.mismatched).getValue() == 0) {
			context.write(new Text(sb.toString()), NullWritable.get());
		} else {
			System.exit(1);
		}
	}


