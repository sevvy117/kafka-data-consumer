package com.kafka.consumer.service;

import com.kafka.consumer.exception.FileException;
import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

@Log4j2
@Service
@RequiredArgsConstructor
public class KafkaConsumerService implements ApplicationListener<ApplicationReadyEvent> {

    private static final String OUTPUT_FILE_NAME = "books.json";

    @Value("${topic-name}")
    private String topicName;
    private final KafkaConsumer<String, KsqlDataSourceSchema> kafkaConsumer;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        var file = new File(OUTPUT_FILE_NAME);
        try {
            file.createNewFile();
        } catch (IOException ex) {
            throw new FileException("Error creating file.", ex);
        }

        try (var writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_NAME, true))) {
            while (true) {
                var records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    String key = record.key();
                    KsqlDataSourceSchema value = record.value();

                    writer.write(value.toString());
                    writer.append(',');
                    writer.newLine();
                    writer.flush();

                    log.info("Consumed record with key {} and value {}, partition: {}, offset: {}", key, value, record.partition(), record.offset());
                }
            }
        } catch (IOException ex) {
            throw new FileException("Error writing to file: ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
