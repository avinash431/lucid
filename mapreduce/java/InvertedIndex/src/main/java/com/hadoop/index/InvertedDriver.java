package com.hadoop.index;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
	
		Job job = Job.getInstance(getConf());
		job.setJarByClass(InvertedDriver.class);
		job.setJobName("Inverted Index ");
		
		Path output = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
		
		job.setMapperClass(InvertedMapper.class);
		job.setReducerClass(InvertedReducer.class);
		job.setCombinerClass(InvertedCombiner.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			int return_code = ToolRunner.run(new InvertedDriver(), args);
			System.exit(return_code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
