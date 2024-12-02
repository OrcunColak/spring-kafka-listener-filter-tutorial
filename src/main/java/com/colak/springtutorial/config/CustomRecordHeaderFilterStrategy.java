package com.colak.springtutorial.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CustomRecordHeaderFilterStrategy implements RecordFilterStrategy<String, String> {

    // If we return true from the filter method then the event is filtered out / skipped.
    // If not it is processed by the consumer.
    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        Headers headers = consumerRecord.headers();
        Header lastHeader = headers.lastHeader("eventType");

        var eventType = new String(lastHeader.value());
        if (eventType.equalsIgnoreCase("ignoreHeader")) {
            log.info("Message skipped because of header");
            return true;
        }

        if (consumerRecord.value().contains("ignore")) {
            log.info("Message skipped because of record");
            return true;
        }
        return false;
    }

}
