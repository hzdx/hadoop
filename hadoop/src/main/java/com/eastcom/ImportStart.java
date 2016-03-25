package com.eastcom;

public class ImportStart {

	public static void main(String[] args){
		String hdfsPath = args[0];
		String sql = args[1];
		System.out.println("开始导入到hdfs................");
		System.out.println("hdfs目录："+hdfsPath);
		System.out.println("导入的数据："+sql);
		DbFileWriter.importQueryResult(sql, hdfsPath);	
		System.out.println("导入完成...........");

	}
	

}
