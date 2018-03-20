package com.hadoop.avro;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class DeserializeNew {

	public static void main(String args[]) throws Exception {
		// Instantiating the Schema.Parser class.
		Schema schema = new Schema.Parser().parse(new File("/Users/avinash/eclipse-workspace/Avro/schema/emp.avsc"));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				new File("/Users/avinash/eclipse-workspace/Avro/schema/mydata.txt"), datumReader);
		GenericRecord emp = null;
		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next(emp);
			System.out.println(emp);
		}
		dataFileReader.close();
		System.out.println("data deserialized wwoooooo...!");
	}


}
