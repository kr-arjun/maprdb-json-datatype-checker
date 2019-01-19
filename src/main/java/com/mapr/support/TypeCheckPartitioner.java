package com.mapr.support;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TypeCheckPartitioner extends Partitioner<ColumnInfoPair, LongWritable> {

    @Override
    public int getPartition(ColumnInfoPair columnInfoPair, LongWritable countValue, int numPartitions) {
        return columnInfoPair.getColumnName().hashCode() % numPartitions;
    }
}