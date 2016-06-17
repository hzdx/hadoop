package com.mycom.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TsvImporterPutMapper;
import org.apache.hadoop.mapreduce.Job;

public class HbaseImporterTest {
	//注意hbase要先建好了表和列簇才能成功
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		conf.addResource("hbase-site.xml");
		conf.set("importtsv.mapper.class", TsvImporterPutMapper.class.getName());
		conf.set("importtsv.rowkey.indexs", "");
		conf.set("importtsv.rowkey.strategies", "|");
		conf.set("importtsv.separator", "");
		// RunJar
		// TableName tableName = TableName.valueOf(args[0]);
		// Path inputDir = new Path(args[1]);
		Job job = ImportTsv.createSubmittableJob(conf, args);
		System.out.println("submit job....");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
