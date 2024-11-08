package com.roy.kfk.basic;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Author： roy
 * Description：
 **/
public class PartitionedProducer {
    private static final String BOOTSTRAP_SERVERS = "worker1:9092,worker2:9092,worker3:9092";
    private static final String TOPIC = "disTopic";
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 配置key的序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 配置value的序列化类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //自定义分区类
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.roy.kfk.basic.MyPartitioner");
//        props.put(ProducerConfig.ACKS_CONFIG,"all");

        Producer<String,String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), "MyProducer" + i);
            //同步发送：获取服务端应答消息前，会阻塞当前线程。
            RecordMetadata recordMetadata = producer.send(record).get();
            String topic = recordMetadata.topic();
            int partition = recordMetadata.partition();
            long offset = recordMetadata.offset();
            String message = recordMetadata.toString();
            System.out.println("message:["+ message+"] sended with topic:"+topic+"; partition:"+partition+ ";offset:"+offset);
        }
        producer.close();
    }
}
