package com.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterOnMultipleColumns {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		ResultScanner rs =null;
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("gender"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Female")));
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("personal"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Prasanna")));
			
			List<Filter> filterList = new ArrayList<Filter>();
			filterList.add(filter);
			filterList.add(filter1);
			
			FilterList filters = new FilterList(filterList);
			
			Scan scan = new Scan();
			scan.setFilter(filters);
			
			 rs = table.getScanner(scan);
			printResults(rs);
		    	
			
		}finally {
			connection.close();
			table.close();
			rs.close();
		}

	}
private static void printResults(ResultScanner rs) {
		
		for(Result result:rs) {
			for(Cell cell:result.listCells()) {
				String rowkey = new String(Bytes.toString(CellUtil.cloneRow(cell)));
				String columnFamily = new String(Bytes.toString(CellUtil.cloneFamily(cell)));
				String columnName = new String(Bytes.toString(CellUtil.cloneQualifier(cell)));
				String value = new String(Bytes.toString(CellUtil.cloneValue(cell)));
				
				System.out.println("Row key is "+ rowkey +" columnFamily is "+columnFamily +"columnName is "+columnName + " value is " +value);
			}
		}
}

}
