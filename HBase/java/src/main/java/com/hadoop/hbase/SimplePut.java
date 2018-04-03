package com.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SimplePut {

	private static byte[] personal_cf = Bytes.toBytes("personal");
	private static byte[] professional_cf = Bytes.toBytes("professional");

	private static byte[] NAME_COLUMN = Bytes.toBytes("name");
	private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
	private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_status");

	private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed");
	private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		Configuration conf = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("census"));
			Put put = new Put(Bytes.toBytes("1"));

			put.addColumn(personal_cf, NAME_COLUMN, Bytes.toBytes("Avinash"));
			put.addColumn(personal_cf, GENDER_COLUMN, Bytes.toBytes("Male"));
			put.addColumn(personal_cf, MARITAL_STATUS_COLUMN, Bytes.toBytes("single"));
			put.addColumn(professional_cf, EMPLOYED_COLUMN, Bytes.toBytes("Yes"));
			put.addColumn(professional_cf, FIELD_COLUMN, Bytes.toBytes("IT"));

			table.put(put);

			Put put1 = new Put(Bytes.toBytes("2"));

			put1.addColumn(personal_cf, NAME_COLUMN, Bytes.toBytes("Prasanna"));
			put1.addColumn(personal_cf, GENDER_COLUMN, Bytes.toBytes("Female"));
			put1.addColumn(personal_cf, MARITAL_STATUS_COLUMN, Bytes.toBytes("single"));
			put1.addColumn(professional_cf, EMPLOYED_COLUMN, Bytes.toBytes("Yes"));
			put1.addColumn(professional_cf, FIELD_COLUMN, Bytes.toBytes("IT"));

			Put put2 = new Put(Bytes.toBytes("3"));

			put2.addColumn(personal_cf, NAME_COLUMN, Bytes.toBytes("Akhila"));
			put2.addColumn(personal_cf, GENDER_COLUMN, Bytes.toBytes("Female"));
			put2.addColumn(personal_cf, MARITAL_STATUS_COLUMN, Bytes.toBytes("single"));

			List<Put> putList = new ArrayList<Put>();
			putList.add(put1);
			putList.add(put2);
			table.put(putList);

			System.out.println("Inserted the rows");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			table.close();
			connection.close();

		}

	}

}
