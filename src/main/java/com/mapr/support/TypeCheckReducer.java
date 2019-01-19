package com.mapr.support;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TypeCheckReducer extends Reducer<ColumnInfoPair, LongWritable, NullWritable, Text> {

	private Text outputValue = new Text();
	private static String DELIMITER = "|";

	@Override
	protected void reduce(ColumnInfoPair key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		Long countValue = 0l;
		String columnName = key.getColumnName().toString();
		String columnType = key.getColumnType().toString();
		StringBuilder builder = new StringBuilder();
		builder.append("columnName=" + columnName).append(DELIMITER);
		for (LongWritable value : values) {
			String currentColumnType = key.getColumnType().toString();
			if (currentColumnType.equals(columnType)) {
				countValue += value.get();
			} else {
				builder.append(columnType + "=" + countValue).append(DELIMITER);
				countValue = value.get();
				columnType = currentColumnType;
			}
		}

		builder.append(columnType + "=" + countValue).append(DELIMITER);

		outputValue.set(builder.toString());
		context.write(NullWritable.get(), outputValue);

	}
}