package com.hadoop.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {
	
	private Text continent;
	private Text country;
	
	

	public Text getContinent() {
		return this.continent;
	}

	public void setContinent(Text continent) {
		this.continent = continent;
	}

	public Text getCountry() {
		return this.country;
	}

	public void setCountry(Text country) {
		this.country = country;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.continent.toString());
		out.writeUTF(this.country.toString());

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.continent=new Text(in.readUTF());
		this.country=new Text(in.readUTF());

	}

	public int compareTo(CustomKey o) {
		// TODO Auto-generated method stub
		int compare = this.getContinent().compareTo(o.getContinent());
		if(compare != 0) {
			return compare;
		}
		return this.getCountry().compareTo(o.getCountry());
	}
	
	

}
