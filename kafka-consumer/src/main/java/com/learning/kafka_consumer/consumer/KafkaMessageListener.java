package com.learning.kafka_consumer.consumer;

import com.learning.kafka_consumer.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
    Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

    /* ---------------------- consumers for partitions ----------------------
    * partition will get assigned to consumer
    * */
    @KafkaListener(topics = "message",groupId = "lk-group")
    public void consume1(String message) {
        logger.info("consumer_1 consumed the message {} ", message);
    }

    @KafkaListener(topics = "message",groupId = "lk-group")
    public void consume2(String message) {
        logger.info("consumer_2 consumed the message {} ", message);
    }

    /* ---------------------- consumer for specific partition only ----------------------
    * consumer to only listen from partition 3
    * */
    @KafkaListener(
            topics = "message",
            groupId = "lk-group",
            topicPartitions = { @TopicPartition(topic = "message", partitions = {"3"})}
    )
    public void consume3(String message) {
        logger.info("consumer_3 consumed the message {} ", message);
    }

    /* ---------------------- consumer for partition of another topic ----------------------
     * partition will get assigned to consumer
     * */
    @KafkaListener(topics = "customer", groupId = "lk-group-1")
    public void consumeEvents(Customer customer) {
        logger.info("consumer consume the events {} ", customer.toString());
    }
}
