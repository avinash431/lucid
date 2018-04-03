package com.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			
			TableName tableName = TableName.valueOf("census");
			
			if(admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				
				System.out.println("Table deleted");
			}
			
		}finally {
			connection.close();
		}

	}

}
