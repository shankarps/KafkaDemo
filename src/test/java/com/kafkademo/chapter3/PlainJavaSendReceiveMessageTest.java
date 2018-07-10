package com.kafkademo.chapter3;

import com.kafkademo.Topology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

/*
 * This Test case runs plain Java code to send and receive a message to Kafka.
 * The Spring Kafka template and Container are created as plain Java objects without
 * any annotations/auto-wiring.
 * This code is the same code from the docs. It has been modified slightly for easier understanding.
 * Refer https://docs.spring.io/spring-kafka/reference/htmlsingle/#_very_very_quick
 */
public class PlainJavaSendReceiveMessageTest {
    Logger logger = LoggerFactory.getLogger(PlainJavaSendReceiveMessageTest.class);

    @ClassRule
    public static KafkaEmbedded kafkaServer = new KafkaEmbedded(1, true, 1, Topology.DEMO_TOPIC);

    @Test
    public void testAutoCommit() throws Exception {
        logger.info("Start auto commit");
        ContainerProperties containerProps = new ContainerProperties(Topology.DEMO_TOPIC);
        CountDownLatch latch = new CountDownLatch(4);
        containerProps.setMessageListener(new MessageListener<Integer, String>() {

            @Override
            public void onMessage(ConsumerRecord<Integer, String> message) {
                logger.info("received: " + message);
                latch.countDown();
            }

        });
        KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
        container.setBeanName("testAuto");
        container.start();
        Thread.sleep(1000); // wait a bit for the container to start
        KafkaTemplate<Integer, String> template = createTemplate();
        template.setDefaultTopic(Topology.DEMO_TOPIC);
        template.sendDefault(0, "foo");
        template.sendDefault(1, "bar");
        template.sendDefault(2, "baz");
        template.sendDefault(3, "qux");
        template.flush();
        assertTrue(latch.await(60, TimeUnit.SECONDS));
        container.stop();
        logger.info("Stop auto");

    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(
            ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf =
                new DefaultKafkaConsumerFactory<Integer, String>(props);
        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private KafkaTemplate<Integer, String> createTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<Integer, String> pf =
                new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        return template;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer.getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
