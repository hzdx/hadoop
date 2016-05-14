package com.mycom.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "/user/ldm/README.md";
		// 没有启动hadoop的话，默认工作目录：file:/home/ldm/，也就是从当前文件夹去寻找文件，但无法使用spark-submit指令
		// 启动hadoop之后，spark默认工作目录 hdfs://node:9000/,这里用的是hdfs目录
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

		/**
		 * 需要上传要hadoop和spark服务本地，执行命令 spark-submit --class
		 * "com.mycom.spark.SimpleApp" --master local hadoop.jar 输出 Lines with
		 * a: 58, lines with b: 26
		 * 
		 * 
		 */
	}
}