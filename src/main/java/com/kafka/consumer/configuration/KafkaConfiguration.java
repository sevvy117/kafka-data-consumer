package com.kafka.consumer.configuration;

import com.kafka.consumer.exception.ConfigException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaConsumer<String, KsqlDataSourceSchema> kafkaConsumer() {
        Properties props = new Properties();

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            var file = new File(classLoader.getResource("consumer.properties").getFile());
            props.load(new FileReader(file));
        } catch (NullPointerException | IOException e) {
            throw new ConfigException("Unable to load properties.", e);
        }

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "books-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}
