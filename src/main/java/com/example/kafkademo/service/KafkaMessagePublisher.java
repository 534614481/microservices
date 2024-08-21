package com.example.kafkademo.service;

import com.example.kafkademo.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${topicName}")
    private String topicName;

    public void getProducerMessage(String message){
        CompletableFuture<SendResult<String, Object>> completableFuture = kafkaTemplate.send(topicName, message);
        completableFuture.whenComplete((result, ex) ->{
            if(ex==null){
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() +" : " +result.getRecordMetadata().partition()+ "]");
        }else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("javatechie-demo", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer.toString() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex) {
            System.out.println("ERROR : "+ ex.getMessage());
        }
    }
}
