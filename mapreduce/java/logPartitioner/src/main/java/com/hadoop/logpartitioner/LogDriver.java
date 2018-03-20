package com.hadoop.logpartitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class LogDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(LogDriver.class);
		job.setJobName("Partition");
		
		job.setMapperClass(LogMapper.class);
		job.setNumReduceTasks(0);
		
		Path input = new Path(arg0[0]);
		Path output = new Path(arg0[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		return job.waitForCompletion(true)? 0 :1 ;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int res = ToolRunner.run(new LogDriver(), args);
			System.exit(res);				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
