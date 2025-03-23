package com.example.kafka_demo_load_test.service;

import com.example.kafka_demo_load_test.model.LoadTestEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
@RequiredArgsConstructor
public class LoadTestService {
    private final KafkaTemplate<String, LoadTestEvent> kafkaTemplate;
    private final String TOPIC = "load.test.topic";
    private final String KEY = "load-test-key";

    public void sendBulkMessages(int count) {
        CountDownLatch latch = new CountDownLatch(count);
        long start = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            LoadTestEvent event = LoadTestEvent.createWithPayload("load-test-" + i, 100 * 1024);
            kafkaTemplate.send(TOPIC, KEY, event).whenComplete((result, ex) -> {
                if (ex != null) {   // ex -> 전송 실패일 때 발생한 예외
                    System.err.println("Failed to send message: " + ex.getMessage());
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Load test interrupted: " + e.getMessage());
        }

        long end = System.currentTimeMillis();
        System.out.println("Sent " + count + " messages in " + (end - start) + " ms");
        System.out.printf("Average time per message: %.3f ms\n\n", (end - start) / (double) count);
    }
}
