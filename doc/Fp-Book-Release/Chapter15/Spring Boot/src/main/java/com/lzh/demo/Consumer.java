package com.lzh.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
@Component
public class Consumer {
    @KafkaListener(topics = {"mytopic"})
    public void consumer(ConsumerRecord consumerRecord) {

        Optional<Object> kafkaMassage = Optional.ofNullable(consumerRecord.value());
        if (kafkaMassage.isPresent()) {
            Object msg = kafkaMassage.get();
            System.out.println(msg);
        }

    }
}
