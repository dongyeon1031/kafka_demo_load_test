package com.example.kafka_demo_load_test.controll;

import com.example.kafka_demo_load_test.service.LoadTestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/loadTest")
public class LoadTestController {

    private final LoadTestService loadTestService;

    @PostMapping()
    public ResponseEntity<String> sendMessages(@RequestParam int count) {
        loadTestService.sendBulkMessages(count);
        return ResponseEntity.ok("Load test started for " + count + " messages.");
    }
}