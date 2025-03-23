package com.example.kafka_demo_load_test.service;

import com.example.kafka_demo_load_test.model.LoadTestEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LoadTestConsumer {
    @KafkaListener(topics = "load.test.topic", groupId = "stock-service", containerFactory = "loadTestKafkaListenerContainerFactory")
    public void consume(LoadTestEvent event) {
        System.out.println(event.getId());
    }
}
