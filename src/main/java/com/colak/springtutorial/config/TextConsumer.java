package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TextConsumer {

    public static final String INPUT_TOPIC = "TEXT-DATA";


    // Read as ConsumerRecord
    @KafkaListener(
            id = "TextConsumerListener",
            topics = INPUT_TOPIC,
            groupId = "TEXT_CONSUMERS",
            filter = "customRecordHeaderFilterStrategy"

    )
    public void consumeMessage(ConsumerRecord<String, String> consumerRecord) {
        log.info("Key : {} Message : {}", consumerRecord.key(), consumerRecord.value());
    }

}