package com.shankar.KafkaDemo.chapter3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.shankar.KafkaDemo.Topology.DEMO_TOPIC;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

/*
 *This class tests the following:
 * 1. Create an embedded Kafka Server
 * 2. Create a Spring Kafka message template and send messages.
 * 3. Create a plain Kafka Consumer and consume the messages.
 * @see https://docs.spring.io/spring-kafka/docs/2.1.6.RELEASE/reference/html/_introduction.html#_introduction
 */

public class PollingConsumerProducerTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1,
            DEMO_TOPIC);
    private static Consumer<String, String> messageConsumer;
    private static KafkaTemplate<String, String> kafkaTemplate;

    @BeforeClass
    public static void setUpKafka() throws Exception {
        Map<String, Object> props = new HashMap<>();
        // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory producerFactory = new DefaultKafkaProducerFactory<>(props);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps = new HashMap<>();
        // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Test_Group");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps);
        messageConsumer = consumerFactory.createConsumer();
        assertNotNull(messageConsumer);
        //embeddedKafka.consumeFromAllEmbeddedTopics(messageConsumer);
        //messageConsumer.subscribe(Arrays.asList(DEMO_TOPIC));
        messageConsumer.subscribe(Arrays.asList(DEMO_TOPIC), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("partitions assigned: " + partitions);
            }
        });
    }

    @Test
    public void verifyKafkaServerPartitions() {
        assert (embeddedKafka.getPartitionsPerTopic() == 1);
    }

    @Test
    public void verifySendReceiveSingleMessage() throws InterruptedException {
        //poll first to consume all messages in pipe - Why should this be done?
        messageConsumer.commitSync();
        ConsumerRecords<String, String> records = messageConsumer.poll(1000);
        assert(records.isEmpty());
        //send a message
        kafkaTemplate.send(DEMO_TOPIC, "Test message from JUnit");
        //poll again
        records = messageConsumer.poll(1000);
        //verify that u received a message
        assert (records.count() == 1);
        for (ConsumerRecord<String, String> record : records) {
            hasValue("Test message from JUnit").matches(record);
        }
        //this is important. Same consumer will be used in other methods, so commit to update the offset.
        messageConsumer.commitSync();
    }

    @Test
    public void verifySendAndPollMultipleMessages() throws InterruptedException {
        //poll first to consume all messages in pipe - Why should this be done?
        ConsumerRecords<String, String> records = messageConsumer.poll(1000);
        assert(records.isEmpty());
        messageConsumer.commitSync();
        //send a message
        kafkaTemplate.send(DEMO_TOPIC, "key1","Test message from JUnit 1" );
        kafkaTemplate.send(DEMO_TOPIC, "key2","Test message from JUnit 2");
        kafkaTemplate.send(DEMO_TOPIC, "key3","Test message from JUnit 3");
        //poll again
        records = messageConsumer.poll(1000);
        //verify that u received the messages
        assertTrue("After first read records count is not 3. It is "+records.count(),records.count() == 3);

        for (ConsumerRecord<String, String> record : records) {
            assertTrue ("Message is not the same as sent message. "+record.value(), record.value().contains("Test message"));
        }
        messageConsumer.commitSync();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        embeddedKafka.destroy();
    }

}
