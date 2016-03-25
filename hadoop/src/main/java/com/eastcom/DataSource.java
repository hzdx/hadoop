package com.eastcom;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class DataSource {
	private static Properties addResource() {
		Properties prop = new Properties();
		InputStream is = DataSource.class.getClassLoader().getResourceAsStream("jdbc.properties");
		try {
			prop.load(is);
			return prop;
		} catch (IOException e) {
			System.out.println("load jdbc config fail!");
			System.exit(1);
			return null;
		}

	}

	public static Connection getConnection() {
		Properties prop = addResource();
		Connection con = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			System.out.println("开始连接数据库.......");
			String url = prop.getProperty("jdbc.url");
			String user = prop.getProperty("jdbc.user");
			String password = prop.getProperty("jdbc.password");
			con = DriverManager.getConnection(url, user, password);
			return con;
		} catch (Exception e) {
			System.out.println("数据库连接失败: "+e.getMessage());
			System.exit(1);
			return null;
		}

	}
	
//	public static void query(String sql){
//		Connection con = getConnection();
//		//String sql = "select * from O_USER_TYPE";
//	    try {
//	    	PreparedStatement pre = con.prepareStatement(sql);
//	    	ResultSet result  = pre.executeQuery();
//	    	// 获取列名
//	        ResultSetMetaData rsmd = result.getMetaData();
//	        int count = rsmd.getColumnCount();
//	        String[] column = new String[count];
//	        for (int i = 0; i < count; i++) {
//	          column[i] = rsmd.getColumnName(i + 1);
//	        }
//
//	        while (result.next()){
//	        	for(String colname:column){
//	        		System.out.print(result.getString(colname)+"\t");
//	        	}
//	        	System.out.println();
//			}
//	        
//		} catch (SQLException e) {
//			System.out.println("查询出错： "+e.getMessage());
//			System.exit(1);
//		} finally{
//			if(con!=null){
//				try{
//					con.close();
//					con = null;
//				}catch(SQLException e){}
//			}
//		}
//}
    

}
