package com.hadoop.Identity;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IdentityDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(getConf());
		job.setJarByClass(IdentityDriver.class);
		job.setJobName("Identity job ");
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int return_code = ToolRunner.run(new IdentityDriver(), args);
			System.exit(return_code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
