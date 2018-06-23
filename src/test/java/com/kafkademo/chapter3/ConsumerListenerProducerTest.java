package com.kafkademo.chapter3;

import com.kafkademo.Topology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/*
 *This class tests the following:
 * 1. Create an embedded Kafka Server
 * 2. Create a Spring Kafka message template and send messages.
 * 3. Create a Spring MessageListener and consume the messages.
 * @see https://docs.spring.io/spring-kafka/docs/2.1.6.RELEASE/reference/html/_introduction.html#_introduction
 */
public class ConsumerListenerProducerTest {

    @ClassRule
    public static KafkaEmbedded kafkaServer = new KafkaEmbedded(1, true, 1, Topology.DEMO_TOPIC);
    private static DefaultKafkaConsumerFactory<Integer, String> consumerFactory;
    private static KafkaMessageListenerContainer<Integer, String> messageListenerContainer;

    private static DefaultKafkaProducerFactory producerFactory;
    private static KafkaTemplate<Integer, String> template;

    private final static CountDownLatch messagesCounter = new CountDownLatch(2);

    @BeforeClass
    public static void configureKafkaServer() throws Exception {
        assert (kafkaServer.getKafkaServers() != null);
        System.out.println(kafkaServer.getKafkaServers());
        assert (kafkaServer.getKafkaServers().size() == 1);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Group1");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        MessageListener listener = message -> {messageCountdown();
                                                System.out.println("received: "+message);};

        ContainerProperties containerProps = new ContainerProperties(Topology.DEMO_TOPIC);

        containerProps.setMessageListener(listener);

        messageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        messageListenerContainer.setBeanName("Message-Listener-Container");
        messageListenerContainer.start();
        Thread.sleep(1000); // wait a bit for the container to start

        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokersAsString());
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.RETRIES_CONFIG, 0);

        producerFactory = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        template = new KafkaTemplate<Integer, String>(producerFactory);
    }

    @Test
    public void testSendReceive() throws Exception {
        template.setDefaultTopic(Topology.DEMO_TOPIC);
        template.sendDefault(0, "Test message 1");
        template.flush();
        template.sendDefault(1, "Test message 2");
        template.flush();
        assertTrue("Error: expected messages not received "+messagesCounter.getCount(),
                messagesCounter.await(10, TimeUnit.SECONDS));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        messageListenerContainer.stop();
        kafkaServer.destroy();
    }

    private static void messageCountdown() {
        messagesCounter.countDown();
    }
}