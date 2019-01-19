package com.mapr.support;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TypeCheckerGroupingComparator extends WritableComparator {
	
	protected TypeCheckerGroupingComparator() {
		super(ColumnInfoPair.class, true); }

	@Override
	public int compare(WritableComparable tp1, WritableComparable tp2) {
		ColumnInfoPair pair1 = (ColumnInfoPair) tp1;
		ColumnInfoPair pair2 = (ColumnInfoPair) tp2;
		return pair1.getColumnName().compareTo(pair2.getColumnName());
	}
}