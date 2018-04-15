package com.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Upper extends UDF {

	public String evaluate(String value) {
		return value.toUpperCase();
	}
}
