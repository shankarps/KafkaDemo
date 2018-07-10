package com.kafkademo.chapter3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.kafkademo.chapter3.SpringConfigSendReceiveMessage.DEMO_TOPIC;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

/* This class tests a simple Kafka consumer and producer created with Spring annotation.
 * The code is adapted from here - https://docs.spring.io/spring-kafka/reference/htmlsingle/#_with_java_configuration
 */
@RunWith(SpringRunner.class)
public class SpringConfigSendReceiveMessage {

    public static final String DEMO_TOPIC =  "demo_topic";

    @Autowired
    private Listener listener;

    @Test
    public void testSimple() throws Exception {
        Thread.sleep(1000);
        template.send(DEMO_TOPIC, 0, "foo");
        template.flush();
        assertTrue(this.listener.latch.await(60, TimeUnit.SECONDS));
    }

    @Autowired
    private KafkaTemplate<Integer, String> template;

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
            //This setting is to allow the consume to consume from the first message.
            //This is needed because the consumer will start after the producer sends and it
            //will not read the read the first message otherwise.
            //Refer https://stackoverflow.com/questions/51219428/kafkalistener-in-unit-test-case-does-not-consume-from-the-container-factory
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
            return factory;
        }

        @Bean
        public Listener listener() {
            return new Listener();
        }

        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplate() {
            return new KafkaTemplate<Integer, String>(producerFactory());
        }
    }



}


class Listener {
    public final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(id = "foo", topics = DEMO_TOPIC)
    public void listen1(String foo) {
        this.latch.countDown();
    }
}
