package com.kafkademo.chapter4.section_4_1_2;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

/**
 *  This class tests the Send message feature of Kafka Template.
 *  It tests the notes from Spring Kafka docs here -  https://docs.spring.io/spring-kafka/reference/htmlsingle/#_sending_messages
 */

@RunWith(SpringRunner.class)
public class KafkaTemplateTest {

    @Autowired
    KafkaEmbedded kafkaServer;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Test
    public void testSendMessage(){
        kafkaTemplate.setDefaultTopic("Topic1");
        kafkaTemplate.sendDefault("Test");
    }

    @Configuration
    @EnableKafka
    public static class Config{

        @Bean
        public KafkaEmbedded kafkaEmbedded(){
            //Create an embedded Kafka server with 1 partition and 1 server.
            return new KafkaEmbedded(1, true, 1);
        }

        @Bean
        public ProducerFactory<Integer, String> producerFactory(){
            return new DefaultKafkaProducerFactory<Integer, String>(producerConfigs());
        }

        public Map<String, Object> producerConfigs(){
            Map<String, Object> props = new HashMap<>();
            //specify the embedded kafka servers's address.
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            //set basic properties.
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return props;
        }

        @Bean
        public KafkaTemplate kafkaTemplate(){
            return new KafkaTemplate(producerFactory());
        }

    }
}


