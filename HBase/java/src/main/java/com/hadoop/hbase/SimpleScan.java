package com.hadoop.hbase;

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

public class SimpleScan {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		ResultScanner resultScanner = null;
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			
			Scan scan = new Scan();
			resultScanner = table.getScanner(scan);
			for(Result result:resultScanner) {
				for(Cell cell : result.listCells()) {
					String row = new String(CellUtil.cloneRow(cell));
					String columnFamily = new String(CellUtil.cloneFamily(cell));
					String columnName = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));
					
					System.out.println("Row is "+row+" column family "+columnFamily+" column Name "+columnName+" value "+value);
							
				}
				
			}
			
			
		}catch (Exception e) {
			// TODO: handle exception
		}finally {
			table.close();
			connection.close();
		}

	}

}
