package com.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<CustomKey,Text> {

	@Override
	public int getPartition(CustomKey key, Text arg1, int numPartitions) {
		// TODO Auto-generated method stub
		return (Math.abs(key.getContinent().hashCode() *127) %numPartitions);
	}

}
