package com.hadoop.join;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJarByClass(JoinDriver.class);
		job.setJobName("Customer Order Join");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		Path customerpath = new Path(arg0[0]);
		Path orderpath = new Path(arg0[1]);
		Path output = new Path(arg0[2]);
		
		MultipleInputs.addInputPath(job, customerpath, TextInputFormat.class,CustomerMapper.class);
		MultipleInputs.addInputPath(job, orderpath, TextInputFormat.class, OrderMapper.class);
		
		job.setReducerClass(CustomerOrderReducer.class);
		//job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, output);
		
		String joinType = arg0[3];
		if (!(joinType.equalsIgnoreCase("inner")
				|| joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter")
				|| joinType.equalsIgnoreCase("fullouter"))) {
			System.err
					.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			System.exit(2);
		}
		
		job.getConfiguration().set("join.type", joinType);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
      try {
		int status = ToolRunner.run(new JoinDriver(), args);
		System.exit(status);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

}