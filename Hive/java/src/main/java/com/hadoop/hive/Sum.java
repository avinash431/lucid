package com.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class Sum extends UDAF {

	public static class SumIntUDAFEvaluator implements UDAFEvaluator {

		private int result;

		public void init() {
			result = 0;
		}

		public boolean iterate(int value) {
			result += value;
			return true;
		}

		public boolean merge(int other) {
			return iterate(other);
		}

		public int terminatePartial() {
			return result;
		}

		public int terminate() {
			return result;
		}

	}

}
