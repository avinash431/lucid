package com.hadoop.bucketing;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BucketDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(getConf());
		job.setJarByClass(BucketDriver.class);
		job.setJobName("Bucketing ");
		
		job.setMapperClass(BucketMapper.class);
		job.setReducerClass(BucketReducer.class);
		//job.setPartitionerClass(BucketPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(7);
		
		Path output = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, output);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		FileSystem fs = FileSystem.get(getConf());
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
		
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			int return_code = ToolRunner.run(new BucketDriver(), args);
			System.exit(return_code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
