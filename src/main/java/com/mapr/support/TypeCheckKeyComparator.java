package com.mapr.support;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TypeCheckKeyComparator extends WritableComparator {

	protected TypeCheckKeyComparator() {
		super(ColumnInfoPair.class, true);
	}

	@Override
	public int compare(WritableComparable tp1, WritableComparable tp2) {
		ColumnInfoPair pair1 = (ColumnInfoPair) tp1;
		ColumnInfoPair pair2 = (ColumnInfoPair) tp2;
		int cmp = pair1.getColumnName().compareTo(pair2.getColumnName());

		if (cmp != 0) {
			return cmp;

		} else {

			return pair1.getColumnType().compareTo(pair2.getColumnType());
		}
	}
}