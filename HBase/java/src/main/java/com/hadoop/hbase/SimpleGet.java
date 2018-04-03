package com.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleGet {
	
	private static byte[] personal_cf = Bytes.toBytes("personal"); 
	private static byte[] professional_cf = Bytes.toBytes("professional");
	
	private static byte[] NAME_COLUMN = Bytes.toBytes("name");
	private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed"); 
	private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			
			Get get = new  Get(Bytes.toBytes("1"));
			get.addColumn(personal_cf, NAME_COLUMN);
			get.addColumn(professional_cf, EMPLOYED_COLUMN);
			
			Result result = table.get(get);
			System.out.println("Name is "+Bytes.toString(result.getValue(personal_cf, NAME_COLUMN)));
			System.out.println("Professional status is "+Bytes.toString(result.getValue(professional_cf, EMPLOYED_COLUMN)));
			
			Get get1 = new Get(Bytes.toBytes("2"));
			get1.addColumn(personal_cf, NAME_COLUMN);
			get1.addColumn(personal_cf, GENDER_COLUMN);
			
			Get get2 = new Get(Bytes.toBytes("3"));
		    get2.addColumn(personal_cf, NAME_COLUMN);
		    get2.addColumn(personal_cf, GENDER_COLUMN);
		    
		    List<Get> getList = new ArrayList<Get>();
		    getList.add(get1);
		    getList.add(get2);
		    
		    Result[] results = table.get(getList);
		    for(Result res:results) {
		    	 System.out.println("Name is "+Bytes.toString(res.getValue(personal_cf, NAME_COLUMN)));
		    	 System.out.println("Gender is "+Bytes.toString(res.getValue(personal_cf, GENDER_COLUMN)));
		    	
		    }

			
			
			
		}catch (Exception e) {
			// TODO: handle exception
		}finally {
			table.close();
			connection.close();
			
		}

	}

}
