package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class JsonProducer {
    private static final Logger LOG = LoggerFactory.getLogger(JsonProducer.class);

    private static final String OUR_BOOTSTRAP_SERVERS = ":9092";

    private static final String OUR_CLIENT_ID = "firstProducer";

    private static Producer<String, String> producer;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, OUR_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, OUR_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,1048576);

        producer = new KafkaProducer<>(props);

        while(true) {
            send("event_json");
            Thread.sleep(3000);
        }
    }

    public static void send(String topic){
        final int number = new Random().nextInt(10);
        var js = "{ \"nume\": \"Popescu\", \"prenume\": \"Alin\", \"functie\": \"Stud_Master_SE\" }";
        ProducerRecord<String, String> data = new ProducerRecord<>(topic, "key"+number, js);
        try {
            RecordMetadata meta = producer.send(data).get();
            LOG.info("key = {}, value = {} ==> partition = {}, offset = {}", data.key(), data.value(), meta.partition(), meta.offset());
        }catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
    }
}