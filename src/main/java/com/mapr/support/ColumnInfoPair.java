package com.mapr.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ColumnInfoPair implements WritableComparable<ColumnInfoPair> {
	
	public ColumnInfoPair() {
		super();
		
	}
	public ColumnInfoPair(String columnName, String columnType) {
		super();
		this.columnName = new Text(columnName);
		this.columnType = new Text(columnType);
	}

	private Text columnName = new Text();
	private Text columnType = new Text();


	public void readFields(DataInput in) throws IOException {
		columnName.readFields(in);
		columnType.readFields(in);

		
	}

	public void write(DataOutput out) throws IOException {
		columnName.write(out);
		columnType.write(out);		
	}

	public Text getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName.set(columnName);
	}

	public Text getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType.set(columnType);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + ((columnType == null) ? 0 : columnType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnInfoPair other = (ColumnInfoPair) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (columnType == null) {
			if (other.columnType != null)
				return false;
		} else if (!columnType.equals(other.columnType))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return " columnName=" + columnName + "/\\ncolumnType=" + columnType + "]";
	}

	public int compareTo(ColumnInfoPair other) {
		int compareValue = this.columnName.compareTo(other.getColumnName());
        if (compareValue == 0) {
            compareValue = this.columnType.compareTo(other.getColumnType());
        }
        return compareValue;
	}


}
