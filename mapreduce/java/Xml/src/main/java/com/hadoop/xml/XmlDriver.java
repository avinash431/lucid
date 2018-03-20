package com.hadoop.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XmlDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(XmlDriver.class);
		job.setJobName("Xml file");
		
		job.setJarByClass(XmlDriver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(XmlMapper.class);
	    job.setInputFormatClass(XmlInputFormat.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(TextOutputFormat.class);
		
	    job.getConfiguration().set("key.value.separator.in.input.line", " ");
	    job.getConfiguration().set("xmlinput.start", "<property>");
	    job.getConfiguration().set("xmlinput.end", "</property>");
	    
		Path input = new Path(arg0[0]);
		Path output = new Path(arg0[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true) ? 0 :1;
	}
	
	public static void main(final String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new XmlDriver(), args);
	    System.exit(res);
	  }

}
