package com.mycom.hadoop.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * java api操作hdfs
 *
 */
public class HdfsTest {

	public static void main(String[] args) throws Exception {
		//String uri = "hdfs://hacluster";
		
		Configuration conf = new Configuration(false);
		conf.addResource("mine/core-site.xml");
		conf.addResource("mine/hdfs-site.xml");
		conf.addResource("mine/mapred-site.xml");
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FileSystem fs = FileSystem.get(conf);
		
		FileSystem fs1 = FileSystem.get(conf);
		
		System.out.println("is same :" + (fs == fs1)); //true
		//fs.close();
		// 写入/java/test.txt文件  默认能生成父目录
//		FSDataOutputStream os = fs.create(new Path("/java/test.txt"));
//		os.write("use java to create hdfs file!".getBytes());
//		os.flush();
//		os.close();
//
//		// 显示/java/test.txt文件的内容
//		InputStream is = fs.open(new Path("/java/test.txt"));
//		IOUtils.copyBytes(is, System.out, 1024, true);
		//可以远程访问hdfs
		String path = "/rawdata/xdr/lte/lte_s1u/lte_s1u_general/20161221/16/15/100_201612211605_020e_12.CSV";
		InputStream is = fs.open(new Path(path));//实际进行网络访问
		
		byte[] buf = new byte[1024];
		int n = 0;
		while((n = is.read(buf)) > 0){
			System.out.print(new String(buf,0,n));
		}
		System.out.println();
		System.out.println("done!");

	}

}
