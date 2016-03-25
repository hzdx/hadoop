package com.mycom.hadoop.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * java api操作hdfs 需要上传jar包到本地，使用hadoop命令或者使用eclipse插件才能运行。
 *
 */
public class HdfsTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String uri = "hdfs://hacluster";
		// 要部署到hadoop上执行,写ip可能会报connected refused异常。
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		// 写入/java/test.txt文件
		FSDataOutputStream os = fs.create(new Path("/java/test.txt"));
		os.write("use java to create hdfs file!".getBytes());
		os.flush();
		os.close();

		// 显示/java/test.txt文件的内容
		InputStream is = fs.open(new Path("/java/test.txt"));
		IOUtils.copyBytes(is, System.out, 1024, true);

	}

}
