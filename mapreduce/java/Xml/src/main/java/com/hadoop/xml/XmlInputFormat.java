package com.hadoop.xml;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		try {
			return new XmlRecordReader((FileSplit) split, context.getConfiguration());
		} catch (IOException ioe) {
			return null;
		}

	}
}