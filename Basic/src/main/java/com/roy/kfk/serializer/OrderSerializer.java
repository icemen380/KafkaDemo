package com.roy.kfk.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class OrderSerializer implements Serializer<Order> {
    @Override
    public byte[] serialize(String topic, Order order) {

        return JSON.toJSONBytes(order);
    }
}
