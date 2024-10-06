package com.learning.kafka_producer.service;

import com.learning.kafka_producer.dto.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaMessagePublisher {
    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("message", message);
        future.whenComplete((stringObjectSendResult, throwable) -> {
            if (throwable == null) {
                logMessage(message, stringObjectSendResult.getRecordMetadata().offset());
            } else {
                errorMessage(message, throwable.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Customer customer) {
        try {
            /*
            * specify in which partition you want to send the message - template.send(topic, partition, key, data)
            * */
            CompletableFuture<SendResult<String, Object>> future = template.send("customer", customer);
            future.whenComplete((stringObjectSendResult, throwable) -> {
                if (throwable == null) {
                    logMessage(customer.toString(), stringObjectSendResult.getRecordMetadata().offset());
                } else {
                    errorMessage(customer.toString(), throwable.getMessage());
                }
            });
        } catch (Exception ex) {
            log.error("ERROR: {}", ex.getMessage());
        }
    }

    private void logMessage(String message, Long offset) {
        log.info(
                "Sent Message=[{}] with offset=[{}]",
                message,
                offset
        );
    }
    private void errorMessage(String message, String error) {
        log.info(
                "Unable to sent message=[{}] dut to=[{}]",
                message,
                error
        );
    }
}
