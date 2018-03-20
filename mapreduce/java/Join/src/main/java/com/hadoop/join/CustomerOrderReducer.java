package com.hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerOrderReducer extends Reducer<Text, Text, Text, Text> {
	
	List<Text> listA = new ArrayList<>();
	List<Text> listB = new ArrayList<>();
	private String joinType = null;
	
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		joinType = context.getConfiguration().get("join.type");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub		
		listA.clear();
		listB.clear();
		
		for (Text text : values) {			
			System.out.println("Text is "+text);
			if(text.toString().charAt(0) == 'c'){
				listA.add(new Text(text.toString().substring(2)));				
			}
			else if(text.toString().charAt(0) == 'o'){
				listB.add(new Text(text.toString().substring(2)));				
			}		
			
		}
		
		  executeJoinLogic(context);
				
			
		}	
	
	private void executeJoinLogic(Context context) throws IOException,
	InterruptedException {
if (joinType.equalsIgnoreCase("inner")) {
	// If both lists are not empty, join A with B
	if (!listA.isEmpty() && !listB.isEmpty()) {
		for (Text A : listA) {
			for (Text B : listB) {
				context.write(context.getCurrentKey(), new Text(A.toString()+","+B.toString()));
			}
		}
	}
} else if (joinType.equalsIgnoreCase("leftouter")) {
	// For each entry in A,
	for (Text A : listA) {
		// If list B is not empty, join A and B
		if (!listB.isEmpty()) {
			for (Text B : listB) {
				context.write(context.getCurrentKey(), new Text(A.toString()+","+B.toString()));
			}
		} else {
			// Else, output A by itself
			context.write(context.getCurrentKey(), new Text(A.toString()));
		}
	}
} else if (joinType.equalsIgnoreCase("rightouter")) {
	// FOr each entry in B,
	for (Text B : listB) {
		// If list A is not empty, join A and B
		if (!listA.isEmpty()) {
			for (Text A : listA) {
				context.write(context.getCurrentKey(), new Text(A.toString()+","+B.toString()));
			}
		} else {
			// Else, output B by itself
			context.write(context.getCurrentKey(), new Text(B.toString()));
		}
	}
} else if (joinType.equalsIgnoreCase("fullouter")) {
	// If list A is not empty
	if (!listA.isEmpty()) {
		// For each entry in A
		for (Text A : listA) {
			// If list B is not empty, join A with B
			if (!listB.isEmpty()) {
				for (Text B : listB) {
					context.write(context.getCurrentKey(), new Text(A.toString()+","+B.toString()));
				}
			} else {
				// Else, output A by itself
				context.write(context.getCurrentKey(), new Text(A.toString()));
			}
		}
	} else {
		// If list A is empty, just output B
		for (Text B : listB) {
			context.write(context.getCurrentKey(), new Text(B.toString()));
		}
	}
}  else {
	throw new RuntimeException(
			"Join type not set to inner, leftouter, rightouter, fullouter, or anti");
}
}
}