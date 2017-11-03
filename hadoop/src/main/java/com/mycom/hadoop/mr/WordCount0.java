package com.mycom.hadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 官方例子：统计文本文件的单词个数列表的mr任务
 *
 */
public class WordCount0 {
//
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		// writable hadoop实现的序列化接口 ，IntWritable 相当于ints
		private Text word = new Text(); // Text 相当于String

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
//
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	//使用hadoop和java命令都可以执行
	//使用hadoop命令，可以获得hadoop配置，classpath
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count1");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/tmp/test.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/test_result1"));
		//输出目录不应该预先存在，防止长时间任务结果被覆盖
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		/**
		 * hadoop执行jar文件: hadoop jar hadoop.jar com.mycom.hadoop.hdfs.WordCount
		 * /mr/input /mr/output/ 16/03/22 06:12:20 INFO client.RMProxy:
		 * Connecting to ResourceManager at /0.0.0.0:8032 Exception in thread
		 * "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output
		 * directory hdfs://localhost:9000/mr/output already exists hadoop
		 * 由于进行的是耗费资源的计算，生产的结果默认是不能被覆盖的， 因此中间结果输出目录一定不能存在，否则出现这个错误。 就是这句代码：
		 * FileOutputFormat.setOutputPath( job, new Path(args[1]) );
		 * 
		 * 使用 hdfs dfs -rmr /mr/output 删除就行了。
		 * 
		 * 
		 */
	}
}