package com.eastcom;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DbFileWriter {
	public static void importQueryResult(String sql,String hdfsPath){
		writeQueryResultToFile(sql, getOuputStream(hdfsPath));
	}
	
	public static FSDataOutputStream getOuputStream(String hdfsPath){
		FSDataOutputStream os = null;
		try {
			System.out.println("正在连接hdfs目录："+hdfsPath);
			String uri = "hdfs://hacluster";
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			os = fs.create(new Path(hdfsPath));
			return os;
		} catch (IOException e) {
			System.out.println("连接hdfs出错："+e.getMessage());
			System.exit(1);
			return null;
		}
	}

	public static void writeQueryResultToFile(String sql,FSDataOutputStream os) {
		Connection con = DataSource.getConnection();
		try {
			System.out.println("开始执行sql:"+sql);
			PreparedStatement pre = con.prepareStatement(sql);
			ResultSet result = pre.executeQuery();
			// 获取列名
			ResultSetMetaData rsmd = result.getMetaData();
			int count = rsmd.getColumnCount();
			String[] column = new String[count];
			for (int i = 0; i < count; i++) {
				column[i] = rsmd.getColumnName(i + 1);
			}
			
			System.out.println("开始写入............");
			while (result.next()) {
				for (String colname : column) {
					os.write((result.getString(colname) + "\t").getBytes());
				}
				os.write("\r\n".getBytes());
			}

		} catch (SQLException e) {
			System.out.println("查询出错： " + e.getMessage());
			System.exit(1);
		} catch (IOException  e) {
			System.out.println("写入文件出错： " + e.getMessage());
			System.exit(1);
		} finally {
			if (con != null) {
				try {
					con.close();
					con = null;
				} catch (SQLException e) {
				}
			}
			try {
				if (os != null) {
					os.flush();
					os.close();
				}
			} catch (IOException ex) {
			}

		}
	}

	public static void main(String[] args) {
		String sql = "select * from O_USER_TYPE";
//		File file = new File("d:/db.txt");
//		if(!file.exists()) file.mkdirs();
		//writeQueryResultToFile(sql, "d:/db.txt");
		

	}

}
