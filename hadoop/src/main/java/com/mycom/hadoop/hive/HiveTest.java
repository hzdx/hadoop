package com.mycom.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * java api操作hive数据库，需要服务端开启hiveserver2服务。 插入数据只能从文件导入，或者从其他表查询得到。 操作类似于jdbc.
 */
public class HiveTest {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String hiveUrl = "jdbc:hive2://192.168.2.105:10000";

	public static void main(String[] args) throws SQLException {

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection conn = DriverManager.getConnection(hiveUrl, "ldm", "mine");
		// 这里用户名要写linux的用户名才有创建表的权限，密码随便写，hive权限不太明白？
		Statement stmt = conn.createStatement();

		// create table
		String tableName1 = "nihao";
		stmt.execute("drop table if exists " + tableName1);
		stmt.execute("create table " + tableName1 + " (key int, value string)");
		System.out.println("Create table success!");
		String tableName = "test";

		// show tables
		String sql = "show tables";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// query
		sql = "select * from " + tableName;
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}

		// mr 任务
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

}
