package org.example.consumer;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.company.Company;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;


public class ObjectConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectConsumer.class);

    private static final String OUR_BOOTSTRAP_SERVERS = ":9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String OUR_CONSUMER_GROUP_ID = "group_1";

    private static KafkaConsumer<String, Company> kafkaConsumer;

    private static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OUR_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, OUR_CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());

        return props;
    }

    public static void pollKafka(String topic) {
        kafkaConsumer.subscribe(Collections.singleton(topic));

        try {
            Duration pollingTime = Duration.of(5, ChronoUnit.SECONDS);
            while (true) {
                ConsumerRecords<String, Company> records = kafkaConsumer.poll(pollingTime);
                // consume the records
                records.forEach(crtRecord -> {
                    LOG.info("------ Simple Example Consumer ------------- topic ={}  key = {}, value = {} => partition = {}, offset = {}",
                            topic, crtRecord.key(), crtRecord.value(), crtRecord.partition(), crtRecord.offset());
                });
            }
        }
        catch(WakeupException e)
        {
            LOG.info("Graceful shutdown of consumer");
        }
    }

    public static void main(String[] args) {
        kafkaConsumer = new KafkaConsumer<>(getKafkaProperties());
        pollKafka("event_company");
    }
}
