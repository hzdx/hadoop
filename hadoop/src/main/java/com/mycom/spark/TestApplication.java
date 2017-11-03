package com.mycom.spark;
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.List;  
import java.util.regex.Pattern;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.storage.StorageLevel;  
import org.apache.spark.streaming.Durations;  
import org.apache.spark.streaming.api.java.JavaDStream;  
import org.apache.spark.streaming.api.java.JavaPairDStream;  
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;  
import org.apache.spark.streaming.api.java.JavaStreamingContext;  
  
import scala.Tuple2;  
  
/** 
 * 测试自定义接收器，多个接收器的数据流合成union成一股数据流。 
 * 
 */  
public class TestApplication {  
  
    private static final Pattern SPACE = Pattern.compile(" ");  
    private static JavaStreamingContext jsc;  
  
    public static void main(String[] args) throws Exception {  
  
        onSparkConf(); // Diver 在驱动节点运行。  
  
        // init(); // Executor 在work节点上运行。根据批处理时间，每隔5秒运行一次。  
  
        initUnionStream();  
  
        startAndWait(); // Diver 在驱动节点运行。  
  
    }  
  
    public static void onSparkConf() {  
  
        System.setProperty("hadoop.home.dir", "E:/doc/hadoop-2.7.2/hadoop-2.7.2");  
  
        // 一个驱动程序占用一个进程，一个接收器占用一个进程。如果local[n] n设置比较小，则只接收，不处理。  
        SparkConf conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[8]");  
        jsc = new JavaStreamingContext(conf, Durations.seconds(5));  
        // jsc.checkpoint("/checkpoint");  
    }  
  
    /** 
     * 单个接收器 
     *  
     *  
     */  
    public static void init() {  
  
        JavaReceiverInputDStream<String> lines = jsc.receiverStream(new FileReceiver(StorageLevel.MEMORY_ONLY()));  
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)));  
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))  
                .reduceByKey((i1, i2) -> i1 + i2);
        System.out.println(words.toString());
        wordCounts.print();  
  
    }  
  
    /** 
     *  
     * 多个接收器 
     *  
     *  
     * 多股流合成一股流 
     *  
     */  
    public static void initUnionStream() {  
  
        List<JavaDStream<String>> streams = new ArrayList<>();  
        JavaDStream<String> lines_1 = jsc.receiverStream(new FileReceiver(StorageLevel.MEMORY_ONLY()));  
        streams.add(lines_1);  
        JavaDStream<String> lines_2 = jsc.receiverStream(new FileReceiver(StorageLevel.MEMORY_ONLY()));  
        streams.add(lines_2);  
        JavaDStream<String> lines_3 = jsc.receiverStream(new FileReceiver(StorageLevel.MEMORY_ONLY()));  
        streams.add(lines_3);  
  
        JavaDStream<String> unifiedStream = jsc.union(streams.get(0), streams.subList(1, streams.size()));  
  
        JavaDStream<String> words = unifiedStream.flatMap(x -> Arrays.asList(SPACE.split(x)));  
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))  
                .reduceByKey((i1, i2) -> i1 + i2);  
  
        wordCounts.print();  
  
    }  
  
    /** 
     * 启动spark, 等待运行终止,关闭spark 
     *  
     */  
    public static void startAndWait() {  
        jsc.start();  
        jsc.awaitTermination();  
        jsc.close();  
    }  
  
}  