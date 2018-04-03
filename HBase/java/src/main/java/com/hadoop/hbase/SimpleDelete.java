package com.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleDelete {
	
	private static byte[] personal_cf = Bytes.toBytes("personal"); 
	private static byte[] professional_cf = Bytes.toBytes("professional");
	
	private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_status");
	private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			
			Delete delete = new Delete(Bytes.toBytes("1"));
			delete.addColumn(personal_cf, MARITAL_STATUS_COLUMN);
			delete.addColumn(professional_cf, FIELD_COLUMN);
			
			table.delete(delete);

	}catch (Exception e) {
		// TODO: handle exception
	}finally {
		connection.close();
		table.close();
	}
 }
}
