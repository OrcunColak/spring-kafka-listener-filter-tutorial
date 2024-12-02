package com.colak.springtutorial.config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class TextConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void testConsume() throws InterruptedException, ExecutionException {
        // Ignored
        sendMessage("key1", "ignore this message");
        // Processed
        sendMessage("key2", "process this message");

        // Ignored
        sendMessageMessageWithHeader("key3", "process this message", "eventType", "ignoreHeader");
        // Processed
        sendMessageMessageWithHeader("key4", "process this message", "eventType", "processHeader");

        TimeUnit.SECONDS.sleep(10);

    }

    private void sendMessage(String key, String payload) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(TextConsumer.INPUT_TOPIC, key, payload).get();
    }

    public void sendMessageMessageWithHeader(String key, String value, String headerKey, String headerValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TextConsumer.INPUT_TOPIC, key, value);
        record.headers().add(headerKey, headerValue.getBytes());
        kafkaTemplate.send(record);
    }


}