package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaManager {
    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaManager(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new KafkaProducer<>(props);
    }

    public void send(byte[] data) {
        producer.send(new ProducerRecord<>(topic, data));
    }
}
