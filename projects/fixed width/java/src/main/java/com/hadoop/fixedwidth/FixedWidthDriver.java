package com.hadoop.fixedwidth;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FixedWidthDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Job job = Job.getInstance(getConf());
		job.setJobName("Fixed Width ");
		job.setJarByClass(getClass());

		job.setMapperClass(FixedWidthMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		FileSystem fs = FileSystem.get(getConf());

		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {

		try {
			int exitCode = ToolRunner.run(new FixedWidthDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
