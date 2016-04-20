package com.mycom.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka生产者api
 */
public class KafkaProducerTest {

	/**
	 * 使用新的api 内部存在一个缓存区 <br>
	 * artifactId=kafka-clients
	 */
	public static void produceUseNewApi(){
		Properties props = new Properties();
		 props.put("bootstrap.servers", "192.168.16.129:9092");
		//此处配置的是kafka的端口
		 props.put("acks", "all");
		 //The acks config controls the criteria under which requests are considered complete. 
		 //The "all" setting we have specified will result in blocking on the full commit of the record, 
		 //the slowest but most durable setting.
		 props.put("retries", 0);//请求失败重试次数为0，如果大于0可能会导致消息重叠
		 props.put("batch.size", 16384);
		 //The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by the batch.size config. Making this larger can result in more batching, but requires more memory 
		 props.put("linger.ms", 1);
		 //默认情况下 client会立即发送消息，即使有可用的缓冲区域。设置大于0的数值(ms)，使得producer等待,批量发送
		 //负载小，可以设置大一些；负载大，应该为0,因为即使为0，到达时间比较相近的消息也会批量发送
		 props.put("buffer.memory", 33554432);//缓冲区大小
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 0; i < 10; i++){
		     producer.send(new ProducerRecord<String, String>("connect-test", "bbb"+Integer.toString(i)));
		     //这里的send方法是异步的，只是发送到了缓冲区并立即返回
		     System.out.println("send :"+Integer.toString(i));
		 }
		 producer.close();
	}
	
	/**
	 * 老的api
	 */
	public static void produceUseOldApi(){
		Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "192.168.16.129:9092");
        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
        int messageNo = 1;
        final int COUNT = 10;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>("connect-test" ,data));
            System.out.println(data);
            messageNo ++;
       }
	}
	
	public static void main(String[] args) {
		produceUseNewApi();
		//produceUseOldApi();
	      
    }

}
