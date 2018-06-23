package com.kafkademo.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
public class NonSpringKafkaConsumer {

    private Properties coustructProperties(String brokers){
        Properties props = new Properties();
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();

        props.put("bootstrap.servers", brokers);
        //create a random group name.
        props.put("group.id", Math.random()+ "-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);

        return props;
    }

    public void consumePrintFromTopic(String brokers, String topic, int pollCount){

        Properties props = coustructProperties(brokers);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (pollCount-- >0) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                        record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            }
        }
    }

    public void consumePrintFromTopic(String brokers, String topic){
        consumePrintFromTopic(brokers, topic, 1);

    }

    public List<ConsumerRecord> consumeMessagesFromTopic(String brokers, String topic, int pollCount, int maxMessages){

        ArrayList<ConsumerRecord> messages = new ArrayList<>();
        Properties props = coustructProperties(brokers);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        int messagesConsumed = 0;
        while (pollCount-- > 0 && (messagesConsumed < maxMessages )) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                messagesConsumed++;
                messages.add(record);
            }
        }

        return messages;
    }

    public void sendMessagetoTopic(String brokers, String topic, String message){
        Properties props = coustructProperties(brokers);
        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord(topic, ""+message.hashCode()+Math.random(), message));
    }

}
