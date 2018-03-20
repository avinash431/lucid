package com.hadoop.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedCombiner extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		List<Text> list = new ArrayList<Text>();
		
		for(Text text:values) {
			list.add(text);
		}
		Set<Text> set = new HashSet<Text>(list);
		Iterator<Text> itr = set.iterator();
		while(itr.hasNext()) {
			context.write(key, itr.next());
		}
		
		
	}

}
