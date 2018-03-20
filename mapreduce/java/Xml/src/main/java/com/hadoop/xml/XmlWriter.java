package com.hadoop.xml;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class XmlWriter extends Reducer<Text, Text, Text, NullWritable> {

	private Text outputKey = new Text();

	protected void setup(Context context) throws IOException, InterruptedException {

		context.write(new Text("<configuration>"), null);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("</configuration>"), null);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			outputKey.set(constructPropertyXml(key, value));

		}
		context.write(outputKey, NullWritable.get());
	}

	public static String constructPropertyXml(Text name, Text value) {
		return String.format("<property><name>%s</name><value>%s</value></property>", name, value);
	}

}