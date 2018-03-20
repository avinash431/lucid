package com.hadoop.wholefilereader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WholeFileRecordDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job =Job.getInstance(getConf());
		
		job.setJarByClass(getClass());
		job.setJobName("Whole file reader");
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequenceFileMapper.class);
		
		Path input = new Path(arg0[0]);
		Path output = new Path(arg0[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int returnCode;
		try {
			returnCode =ToolRunner.run(new WholeFileRecordDriver(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
