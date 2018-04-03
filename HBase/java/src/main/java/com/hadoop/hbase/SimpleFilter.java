package com.hadoop.hbase;

import java.io.IOException;

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
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleFilter {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			
			Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1")));
			
			Scan scan1 = new Scan();
			scan1.setFilter(filter1);
			
			ResultScanner rs = table.getScanner(scan1);
			
			for(Result result:rs) {
				for(Cell cell:result.listCells()) {
					String rowkey = new String(Bytes.toString(CellUtil.cloneRow(cell)));
					String columnFamily = new String(Bytes.toString(CellUtil.cloneFamily(cell)));
					String columnName = new String(Bytes.toString(CellUtil.cloneQualifier(cell)));
					String value = new String(Bytes.toString(CellUtil.cloneValue(cell)));
					
					System.out.println("Row key is "+ rowkey +" columnFamily is "+columnFamily +"columnName is "+columnName + " value is " +value);
				}
			}
			
			Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("3")));
			
			Scan scan2 = new Scan();
			scan1.setFilter(filter2);
			
			ResultScanner rs2 = table.getScanner(scan2);
			
			for(Result result:rs2) {
				for(Cell cell:result.listCells()) {
					String rowkey = new String(Bytes.toString(CellUtil.cloneRow(cell)));
					String columnFamily = new String(Bytes.toString(CellUtil.cloneFamily(cell)));
					String columnName = new String(Bytes.toString(CellUtil.cloneQualifier(cell)));
					String value = new String(Bytes.toString(CellUtil.cloneValue(cell)));
					
					System.out.println("Row key is "+ rowkey +" columnFamily is "+columnFamily +"columnName is "+columnName + " value is " +value);
				}
			}
			
			
			
		}finally {
			connection.close();
			table.close();
		}

	}

}
