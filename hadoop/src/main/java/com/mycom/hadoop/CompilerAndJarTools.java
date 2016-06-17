package com.mycom.hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class CompilerAndJarTools {
	private String javaSourcePath;
	private String javaClassPath;
	private String targetPath;

	public CompilerAndJarTools(String javaSourcePath, String javaClassPath, String targetPath) {
		this.javaSourcePath = javaSourcePath;
		this.javaClassPath = javaClassPath;
		this.targetPath = targetPath;
	}

	// 要编译通过必须有classpath环境
	public void complier() throws IOException {

		System.out.println("*** --> 开始编译java源代码...");

		File javaclassDir = new File(javaClassPath);
		if (!javaclassDir.exists()) {
			javaclassDir.mkdirs();
		}

		List<String> javaSourceList = new ArrayList<String>();
		getFileList(new File(javaSourcePath), javaSourceList);

		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		int result = -1;
		for (int i = 0; i < javaSourceList.size(); i++) {
			result = javaCompiler.run(null, null, null, "-d", javaClassPath, javaSourceList.get(i));
			System.out.println(
					result == 0 ? "*** 编译成功 : " + javaSourceList.get(i) : "### 编译失败 : " + javaSourceList.get(i));
		}
		System.out.println("*** --> java源代码编译完成。");
	}

	private void getFileList(File file, List<String> fileList) throws IOException {

		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					getFileList(files[i], fileList);
				} else {
					fileList.add(files[i].getPath());
				}
			}
		} else if (file.isFile())
			fileList.add(file.getPath());
	}

	public void generateJar() throws FileNotFoundException, IOException {

		System.out.println("*** --> 开始生成jar包...");
		String targetDirPath = targetPath.substring(0, targetPath.lastIndexOf("/"));
		File targetDir = new File(targetDirPath);
		if (!targetDir.exists()) {
			targetDir.mkdirs();
		}

		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

		JarOutputStream target = new JarOutputStream(new FileOutputStream(targetPath), manifest);
		writeClassFile(new File(javaClassPath), target);
		target.close();
		System.out.println("*** --> jar包生成完毕。");
	}

	private void writeClassFile(File source, JarOutputStream target) throws IOException {
		BufferedInputStream in = null;
		try {
			if (source.isDirectory()) {
				// String name = source.getPath().replace("\\", "/");
				// if (!name.isEmpty()) {
				// if (!name.endsWith("/")) {
				// name += "/";
				// }
				// name = name.substring(javaClassPath.length());
				// JarEntry entry = new JarEntry(name);
				// entry.setTime(source.lastModified());
				// target.putNextEntry(entry);
				// target.closeEntry();
				// }
				for (File nestedFile : source.listFiles())
					writeClassFile(nestedFile, target);
				return;
			}

			String middleName = source.getPath().replace("\\", "/").substring(javaClassPath.length() + 1);
			JarEntry entry = new JarEntry(middleName);
			entry.setTime(source.lastModified());
			target.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(source));

			byte[] buffer = new byte[1024];
			while (true) {
				int count = in.read(buffer);
				if (count == -1)
					break;
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		} finally {
			if (in != null)
				in.close();
		}
	}

	public static void main(String[] args) throws IOException {
		// String currentDir = "E:\\github\\casual\\Casual";
		// String javaSourcePath = currentDir + "/src/main/java/";
		// String javaClassPath = currentDir + "/classes";
		// String targetPath = currentDir + "/target/MyProject.jar";

		String javaSourcePath = "E:\\github\\hadoop\\hadoop\\src\\main\\java\\org\\apache\\hadoop\\hbase\\mapreduce\\TsvImporterPutMapper.java";
		String javaClassPath = "D:/tmp/classes1";
		String targetPath = "D:/tmp/TsvImporterPutMapper.jar";

		CompilerAndJarTools cl = new CompilerAndJarTools(javaSourcePath, javaClassPath, targetPath);
		cl.complier();
		cl.generateJar();

	}

}
