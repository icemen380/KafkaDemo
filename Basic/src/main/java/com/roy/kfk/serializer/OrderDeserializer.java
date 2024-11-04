package com.roy.kfk.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;

public class OrderDeserializer implements Deserializer<Order> {
    @Override
    public Order deserialize(String s, byte[] bytes) {
        return JSON.parseObject(bytes, Order.class);
    }
}
