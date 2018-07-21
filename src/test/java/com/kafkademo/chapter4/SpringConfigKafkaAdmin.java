package com.kafkademo.chapter4;

import com.sun.org.apache.bcel.internal.generic.NEW;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;
import scala.collection.Seq;
import scala.collection.Set;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

/* This class tests a Kafka Admin object and automatic topic creation with NewTopic objects.
 * The code is adapted from Chapter 4.1.1 - https://docs.spring.io/spring-kafka/reference/htmlsingle/#_configuring_topics
 */
@RunWith(SpringRunner.class)
public class SpringConfigKafkaAdmin {
    @Autowired
    KafkaEmbedded kafkaServer;

    @Autowired
    KafkaAdmin admin;

    @Test
    public void testTopicsCreated() throws Exception {
        //verify that there is a Kafka Server
        assertTrue(kafkaServer.getKafkaServers() != null);
        assertTrue(kafkaServer.getKafkaServers().size() == 1);

        //Verify that 2 topics are created.
        Seq<String> topics = kafkaServer.getKafkaServers().get(0).zkUtils().getAllTopics();
        assertTrue(topics.size() == 2);
        assert (topics.contains("Topic1"));
        assert (topics.contains("Topic2"));

        //verify that there are total of 6 partitions (3 for each topic)
        Set<TopicAndPartition> topicPartitions = kafkaServer.getKafkaServers().get(0).zkUtils().getAllPartitions();
        assertTrue(topicPartitions.size() == 6);
    }

    @Test
    public void testAdvancedFeatures() throws Exception {
        //Create an Admin client for advanced ops.
        AdminClient adminClient = AdminClient.create(admin.getConfig());

        //Verify if the auto wired topic is created
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        assert(listTopicsResult.names().get().contains("Topic1"));

        //Add a new topic and wait 1 sec.
        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic("Topic3", 1, (short)1));
        adminClient.createTopics(newTopics);
        Thread.sleep(500);

        //Verify that the new Topic is created.
        Seq<String> topics = kafkaServer.getKafkaServers().get(0).zkUtils().getAllTopics();
        assert(topics.contains("Topic3"));

        //Delete the topics
        List<String> topicNames = new ArrayList<>();
        topicNames.add("Topic3");
        adminClient.deleteTopics(topicNames);
        Thread.sleep(500);

        //Confirm that the topic is deleted.
        topics = kafkaServer.getKafkaServers().get(0).zkUtils().getAllTopics();
        assert(!topics.contains("Topic3"));
    }

    @Configuration
    @EnableKafka
    public static class Config {

        @Bean
        public KafkaEmbedded kafkaEmbedded() {
            return new KafkaEmbedded(1, true, 2);
        }

        //Since we have a KafkaAdmin object, we can automatically add new topics.
        @Bean
        public KafkaAdmin admin() {
            Map<String, Object> configs = new HashMap<>();
            configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.arrayToCommaDelimitedString(kafkaEmbedded().getBrokerAddresses()));

            return new KafkaAdmin(configs);
        }

        //Creates a topic
        @Bean
        public NewTopic topic1() {
            return new NewTopic("Topic1", 3, (short) 1);
        }

        //Creates a topic
        @Bean
        public NewTopic topic2() {
            return new NewTopic("Topic2", 3, (short) 1);
        }

    }

}


