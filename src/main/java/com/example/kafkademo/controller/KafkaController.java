package com.example.kafkademo.controller;

import com.example.kafkademo.dto.Customer;
import com.example.kafkademo.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaController {
    @Autowired
    private KafkaMessagePublisher kmp;
    @GetMapping("/kafka/{msg}")
    public ResponseEntity<?> getProducerMessage(@PathVariable String msg){
        try{
            for(int i=0;i<100000;i++){
                kmp.getProducerMessage(msg);
            }
            return ResponseEntity.ok("messaged published successfully ..");
        }catch(Exception ex){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish")
    public void sendEvents(@RequestBody Customer customer) {
        kmp.sendEventsToTopic(customer);
    }
}
