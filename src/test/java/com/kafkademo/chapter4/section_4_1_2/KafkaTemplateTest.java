package com.kafkademo.chapter4.section_4_1_2;

import com.kafkademo.Topology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class tests the Send message feature of Kafka Template.
 * It tests the notes from Spring Kafka docs here -  https://docs.spring.io/spring-kafka/reference/htmlsingle/#_sending_messages
 */

@RunWith(SpringRunner.class)
public class KafkaTemplateTest {

    Logger logger = LoggerFactory.getLogger(KafkaTemplateTest.class);

    @Autowired
    KafkaEmbedded kafkaServer;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    DemoTopicConsumer consumer;

    @Autowired
    CustomProducerListener customProducerListener;

    @Before
    public void resetListenerMessages() {
        consumer.setMessages(new ArrayList<ConsumerRecord>());
    }

    /**
     * Test the ListenableFuture<SendResult<K, V>> sendDefault(V data) method.
     * Default key.
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendDataToDefaultTopic() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.sendDefault("Test message with default key");
        kafkaTemplate.flush();
        Thread.sleep(500);
        assert (consumer.getMessages().size() == 1);
        assert (consumer.getMessages().get(0).value().equals("Test message with default key"));
        logger.info("Default Key is " + consumer.getMessages().get(0).key());
    }

    /**
     * Test the ListenableFuture<SendResult<K, V>> sendDefault(K key, V data); method.
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendKeyAndDataToDefaultTopic() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.sendDefault("key1", "Test message with key and topic");
        kafkaTemplate.flush();
        Thread.sleep(500);
        assert (consumer.getMessages().size() == 1);
        assert (consumer.getMessages().get(0).value().equals("Test message with key and topic"));
        assert (consumer.getMessages().get(0).key().equals("key1"));
    }

    /**
     * Test the ListenableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, V data); method.
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendKeyDataToDefaultTopicWithPartition() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.sendDefault(0, "key1", "Test message with key and topic");
        kafkaTemplate.flush();
        Thread.sleep(500);
        assert (consumer.getMessages().size() == 1);
        assert (consumer.getMessages().get(0).value().equals("Test message with key and topic"));
        assert (consumer.getMessages().get(0).key().equals("key1"));
    }

    /**
     * Test the ListenableFuture<SendResult<K, V>> send(String topic, K key, V data); method.
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendTopicDataToDefaultTopicWithPartition() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.send("Topic2",0, "key1", "Test message with key and topic");
        kafkaTemplate.flush();
        Thread.sleep(500);
        //check that the consumer got
        assert (consumer.getMessages().size() == 1);
        assert (consumer.getMessages().get(0).value().equals("Test message with key and topic"));
        assert (consumer.getMessages().get(0).key().equals("key1"));
    }

    /**
     * Test the metrics() method.
     *
     * @throws InterruptedException
     */
    @Test
    public void testMerics() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.sendDefault("key1", "Test message with key and topic");
        kafkaTemplate.sendDefault("key2", "Test message with key and topic");
        kafkaTemplate.sendDefault("key3", "Test message with key and topic");

        kafkaTemplate.flush();
        Thread.sleep(500);

        Map metrics = kafkaTemplate.metrics();
        //check that there are metrics
        assert(metrics != null);

    }

    /**
     * Test the partitionsFor() method.
     *
     * @throws InterruptedException
     */
    @Test
    public void testPartitionsFor() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.sendDefault("key1", "Test message with key and topic");
        kafkaTemplate.sendDefault("key2", "Test message with key and topic");
        kafkaTemplate.sendDefault("key3", "Test message with key and topic");

        kafkaTemplate.flush();
        Thread.sleep(500);

        List<PartitionInfo> partitionInfo = kafkaTemplate.partitionsFor(Topology.DEMO_TOPIC);
        assert(partitionInfo != null);
        //check that merics exist for 1 partition (configured for this topic)
        assert(partitionInfo.size() ==1);

    }

    @Test
    public void testProducerListener() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        kafkaTemplate.setProducerListener(customProducerListener);
        kafkaTemplate.sendDefault("key1", "Test message with key and topic");
        kafkaTemplate.sendDefault("key2", "Test message with key and topic");
        kafkaTemplate.sendDefault("key3", "Test message with key and topic");

        kafkaTemplate.flush();
        Thread.sleep(500);

        //verify listener has messages
        assert(customProducerListener.getMessages() != null);

        //verify listener has 3 messages
        assert(customProducerListener.getMessages().size() == 3);

    }

    @Test
    public void testListenableFuture() throws InterruptedException {
        kafkaTemplate.setDefaultTopic(Topology.DEMO_TOPIC);
        //Send a message and handle status async.
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.sendDefault("key1", "Test message with key and topic");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {

            }

            @Override
            public void onSuccess(@Nullable SendResult<String, String> stringStringSendResult) {

            }
        });

        //Send a message and handle status blocking (sync).
        try {
            kafkaTemplate.sendDefault("key2", "Test message with key and topic").get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


        kafkaTemplate.flush();
        Thread.sleep(500);
    }

    @Configuration
    @EnableKafka
    public static class Config {

        @Bean
        public KafkaEmbedded kafkaEmbedded() {
            //Create an embedded Kafka server with 1 partition and 1 server.
            return new KafkaEmbedded(1, true, 1);
        }

        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            return new DefaultKafkaProducerFactory<Integer, String>(producerConfigs());
        }

        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            //specify the embedded kafka servers's address.
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            //set basic properties.
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return props;
        }

        @Bean
        public DemoTopicConsumer listener() {
            return new DemoTopicConsumer();
        }

        @Bean
        public CustomProducerListener customProducerListener() {
            return new CustomProducerListener();
        }

        @Bean
        public KafkaTemplate kafkaTemplate() {
            KafkaTemplate kafkaTemplate = new KafkaTemplate(producerFactory());
            return kafkaTemplate;
        }

        @Bean
        public ConsumerFactory<Integer, String> createConsumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            //This setting is to allow the consume to consume from the first message.
            //This is needed because the consumer will start after the producer sends and it
            //will not read the read the first message otherwise.
            //Refer https://stackoverflow.com/questions/51219428/kafkalistener-in-unit-test-case-does-not-consume-from-the-container-factory
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(createConsumerFactory());
            return factory;
        }

    }
}

class CustomProducerListener implements ProducerListener<String, String> {

    private HashMap<String, String> messages = new HashMap<>();

    @Override
    public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
        messages.put(key, value);
    }

    @Override
    public void onError(String s, Integer integer, String o, String o2, Exception e) {
    }

    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }

    public HashMap<String, String> getMessages() {
        return messages;
    }
}

class DemoTopicConsumer {

    private List<ConsumerRecord> messages = new ArrayList<>();

    @KafkaListener(id = "foo", topics = {Topology.DEMO_TOPIC, "Topic2"})
    public void listen1(ConsumerRecord message) {
        messages.add(message);
    }

    public List<ConsumerRecord> getMessages() {
        return messages;
    }

    public void setMessages(List<ConsumerRecord> messages) {
        this.messages = messages;
    }

}


