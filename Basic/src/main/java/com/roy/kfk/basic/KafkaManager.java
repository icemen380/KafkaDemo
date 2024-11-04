package com.roy.kfk.basic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Author： roy
 * Description：
 **/
public class KafkaManager {
    public static final String topicName = "disTopic";
    public static final int numPartitions = 3;
    public static final short replicationFactor = 2;

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.65.112:9092");

        AdminClient admin = AdminClient.create(config);

        NewTopic topic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            // 创建Topic
//            admin.createTopics(Collections.singleton(topic)).all().get();
//            System.out.println("Topic created successfully");

            //列出所有Topic
//            KafkaFuture<Set<String>> names = admin.listTopics().names();
//            names.get().forEach(System.out::println);
            //列出Topic下的所有Partition
            KafkaFuture<Map<String, TopicDescription>> disTopic = admin.describeTopics(Collections.singleton("disTopic")).allTopicNames();
            Map<String, TopicDescription> partitionsMap = disTopic.get();
            partitionsMap.forEach((k, v) -> {
                System.out.println(k + ":" + v);
                v.partitions().forEach(p -> {
                    System.out.println(p.partition());
                    System.out.println(p.leader());
                    System.out.println(p.replicas());
                    System.out.println(p.isr());
                });
            });
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error occurred while creating the topic: " + e.getMessage());
        }

        admin.close();
    }
}
