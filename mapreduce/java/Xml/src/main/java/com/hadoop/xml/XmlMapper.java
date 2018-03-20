package com.hadoop.xml;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String document = value.toString();
	      System.out.println("'" + document + "'");
	      try {
	        XMLStreamReader reader =
	            XMLInputFactory.newInstance().createXMLStreamReader(new
	                ByteArrayInputStream(document.getBytes()));
	        String propertyName = "";
	        String propertyValue = "";
	        String currentElement = "";
	        while (reader.hasNext()) {
	          int code = reader.next();
	          switch (code) {
	            case START_ELEMENT:
	              currentElement = reader.getLocalName();
	              break;
	            case CHARACTERS:
	              if (currentElement.equalsIgnoreCase("name")) {
	                propertyName += reader.getText();
	              } else if (currentElement.equalsIgnoreCase("value")) {
	                propertyValue += reader.getText();
	              }
	              break;
	          }
	        }
	        reader.close();
	        context.write(new Text(propertyName.trim()), new Text(propertyValue.trim()));
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	}

}