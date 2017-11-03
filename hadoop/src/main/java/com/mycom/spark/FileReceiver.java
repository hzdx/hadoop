package com.mycom.spark;
import java.io.InputStream;  
import java.util.List;  
  
import org.apache.commons.io.IOUtils;  
import org.apache.spark.storage.StorageLevel;  
import org.apache.spark.streaming.receiver.Receiver;  
  
public class FileReceiver extends Receiver<String> {  
  
    private static final long serialVersionUID = 1L;  
  
    public FileReceiver(StorageLevel storageLevel) {  
        super(storageLevel);  
    }  
  
    /** 
     * 在次方法中一般是启动一个线程，线程不断的读取数据源往store中丢数据。 
     *  
     * 接收到是通过jsc.receiverStream(new FileReceiver(StorageLevel.MEMORY_ONLY())) 
     *  
     * 从store 中取得数据。 
     *  
     */  
    @Override  
    public void onStart() {  
        new Worker().start();  
        // store("Hello World");  
    }  
  
    @Override  
    public void onStop() {  
  
    }  
  
    class Worker extends Thread {  
  
        @Override  
        public void run() {  
            while (true) {  
                try {  
                    InputStream resource = FileReceiver.class.getClassLoader().getResourceAsStream("content.txt");
                    List<String> lines = IOUtils.readLines(resource);  
                    for (String line : lines) {
                    	System.out.println("sent " + line); 
                        store(line); // 往spark中丢数据  
                         
                        Thread.sleep(1000);  
                    }  
                } catch (Exception e) {  
                    e.printStackTrace();  
                }  
            }  
        }  
  
    }  
  
    public static void main(String[] args) {  
  
        FileReceiver fileReceiver = new FileReceiver(StorageLevel.MEMORY_ONLY());  
        fileReceiver.onStart();  
  
    }  
  
}  