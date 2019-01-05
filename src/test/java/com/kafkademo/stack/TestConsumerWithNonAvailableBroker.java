package com.kafkademo.stack;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

/* This class test the case where the broker becomes unavailable and the consumer sends a NonResponsiveConsumerEvent.
 * I created this class to answer this StackOverflow question -
 * https://stackoverflow.com/questions/53947559/how-to-catch-warning-broker-may-not-be-available-at-the-spring-kafka-listener
 */
@RunWith(SpringRunner.class)
public class TestConsumerWithNonAvailableBroker {

    public static final String DEMO_TOPIC = "demo_topic";

    @Autowired
    Listener listener;

    @Autowired
    KafkaEmbedded kafkaEmbedded;

    @Test
    public void testSimple() throws Exception {
        Thread.sleep(1000);
        //Stop the embedded Kafka server.
        kafkaEmbedded.destroy();
        //Now wait for the NonResponsiveConsumerEvent error to be thrown in Listener.
        Thread.sleep(300000L);

    }

    @Configuration
    @EnableKafka
    public static class Config {

        @Bean
        public KafkaEmbedded kafkaEmbedded() {
            return new KafkaEmbedded(1, true, 1, DEMO_TOPIC);
        }

        @Bean
        public ConsumerFactory<Integer, String> createConsumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(createConsumerFactory());

            factory.getContainerProperties().setIdleEventInterval(10L);
            factory.getContainerProperties().setNoPollThreshold(1L);
            factory.getContainerProperties().setMonitorInterval(1);

            return factory;
        }

        @Bean
        public Listener listener() {
            return new Listener();
        }

    }
}

