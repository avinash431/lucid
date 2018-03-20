package com.hadoop.Text2SequenceFile;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Text2SequenceFile extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		job.setJobName("Text to Sequence File");
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
        Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
		
		FileSystem fs = FileSystem.get(getConf());
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
		
		return job.waitForCompletion(true) ? 0 :1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			int return_code = ToolRunner.run(new Text2SequenceFile(), args);
			System.exit(return_code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
