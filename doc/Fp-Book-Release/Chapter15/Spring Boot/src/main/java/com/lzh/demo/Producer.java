package com.lzh.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ServletContextAware;

import javax.servlet.ServletContext;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */

@Component
public class Producer  implements ServletContextAware {
    @Autowired
    private KafkaTemplate kafkaTemplate;



    @Override
    public void setServletContext(ServletContext servletContext) {
        System.out.println("ApplicationRunner的run方法");
        kafkaTemplate.send("mytopic","Some message");

    }
}
