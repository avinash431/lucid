package com.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalOrderPartitionerExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		// Create job and parse CLI parameters

		try {
			int return_code = ToolRunner.run(new TotalOrderPartitionerExample(), args);
			System.exit(return_code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Path input = new Path(args[0]);
		Path partitionFile = new Path(args[1]);
		Path output = new Path(args[2]);

		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);

		//Configuration conf = super.getConf();

		Job job = new Job(getConf());
		job.setJarByClass(TotalOrderPartitionerExample.class);

		job.setNumReduceTasks(2);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		InputSampler.writePartitionFile(job, sampler);

		URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
		DistributedCache.addCacheFile(partitionUri, getConf());
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
