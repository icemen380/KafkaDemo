package com.roy.kfk.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Author： roy
 * Description：将INPUT_TOPIC中每个单词出现的次数。
 **/
public class WordCountStream {

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.65.112:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaStreams streams = new KafkaStreams(buildTopology(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // 优雅关闭。streams需要调用close才会清除本地缓存
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Object, String> source = streamsBuilder.stream(WordCountStream.INPUT_TOPIC);
        //flatMapValues：对每个值(如果Value是Collection，也会解析出每个值)执行一个函数，返回一个或多个值
        // 将字符串转换为小写，并使用空格分隔符分割字符串
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // 将每个单词作为key，进行分组
                .groupBy((key, value) -> value)
                // 对每个分组进行计数，结果为一个KTable，可以理解为一个中间结果集
                .count()
                // 转换成为KStream数据流
                .toStream()
                // 输出到指定Topic
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(),Serdes.Long()));
        return streamsBuilder.build();
    }
}
