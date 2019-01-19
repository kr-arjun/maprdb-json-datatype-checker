package com.mapr.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.ojai.Document;
import org.ojai.Value;

public class TypeCheckMapper extends Mapper<Value, Document, ColumnInfoPair, LongWritable> {

	private Map<ColumnInfoPair, LongWritable> typeCountMap = new HashMap<ColumnInfoPair, LongWritable>();
	private boolean checkAllColumns = true;
	ColumnInfoPair infoPair = new ColumnInfoPair();
	List<String> columnList;

	@Override
	protected void cleanup(Mapper<Value, Document, ColumnInfoPair, LongWritable>.Context context)
			throws IOException, InterruptedException {

		for (ColumnInfoPair colTypePair : typeCountMap.keySet()) {

			context.write(colTypePair, typeCountMap.get(colTypePair));
		}

	}

	@Override
	protected void setup(Mapper<Value, Document, ColumnInfoPair, LongWritable>.Context context)
			throws IOException, InterruptedException {

		String columns = context.getConfiguration().get("columns");
		if (StringUtils.isNotBlank(columns)) {
			columnList = new ArrayList<String>(Arrays.asList(columns.split(" , ")));
			checkAllColumns = false;
		}

	}

	@Override
	public void map(Value key, Document record, Context context) throws IOException, InterruptedException {
		Map<String, Object> docMap = record.asMap();

		for (String colName : docMap.keySet()) {
			if (checkAllColumns == false && !columnList.contains(colName)) {
				continue;
			}
			String type = docMap.get(colName).getClass().getName();
			infoPair.setColumnName(colName);
			infoPair.setColumnType(type);
			LongWritable countValue = typeCountMap.get(infoPair);
			if (countValue == null) {
				typeCountMap.put(new ColumnInfoPair(colName, type), new LongWritable(1l));
			} else {
				countValue.set(countValue.get() + 1);

			}
		}
	}
}