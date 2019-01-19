package com.mapr.support;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.db.mapreduce.MapRDBMapReduceUtil;

public class MaprDBJsonTypeFinder extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(MaprDBJsonTypeFinder.class);
	private static String tableName;
	private final static String NAME = "MaprDBJsonTypeCheck";

	private static Job createSubmittableJob(Configuration conf, String[] otherArgs) throws IOException {

		tableName = otherArgs[0];

		if (otherArgs.length > 2) {
			conf.set("column", otherArgs[2]);
		}

		Job job = Job.getInstance(conf, NAME + "_" + tableName);
		job.setJarByClass(MaprDBJsonTypeFinder.class);
		MapRDBMapReduceUtil.configureTableInputFormat(job, tableName);
		job.setMapperClass(TypeCheckMapper.class);
		job.setReducerClass(TypeCheckReducer.class);

		job.setMapOutputKeyClass(ColumnInfoPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setSortComparatorClass(TypeCheckKeyComparator.class);
		job.setGroupingComparatorClass(TypeCheckerGroupingComparator.class);
		job.setPartitionerClass(TypeCheckPartitioner.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// job.setNumReduceTasks(0);

		return job;
	}

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("USage : MaprDBJsonTypeCheck <table> <outputDir> [column] ");
			System.exit(-1);
		}

		Job job = createSubmittableJob(getConf(), otherArgs);
		if (!job.waitForCompletion(true)) {
			LOG.error("MaprDBJsonTypeCheck mapreduce job failed !!");
			return 1;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = 0;
		try {
			ret = ToolRunner.run(new Configuration(), new MaprDBJsonTypeFinder(), args);
		} catch (Exception e) {
			ret = 1;
			e.printStackTrace();
		}
		System.exit(ret);
	}

}
