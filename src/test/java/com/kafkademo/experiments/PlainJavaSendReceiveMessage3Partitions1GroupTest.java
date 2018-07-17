package com.kafkademo.experiments;

import com.kafkademo.Topology;
import com.kafkademo.chapter3.PlainJavaSendReceiveMessageTest;
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
 * This Test case runs plain Java code to send and receive a message to Kafka with 3 partitions and 3 consumers
 * of the same Group.
 * Each listener gets one message each, sent to its partition.
 */
public class PlainJavaSendReceiveMessage3Partitions1GroupTest {
    Logger logger = LoggerFactory.getLogger(PlainJavaSendReceiveMessageTest.class);

    //Start the Kafka Server with 3 partitions.
    @ClassRule
    public static KafkaEmbedded kafkaServer = new KafkaEmbedded(1, true, 3, Topology.DEMO_TOPIC);

    @Test
    public void testAutoCommit() throws Exception {
        logger.info("Start auto commit");
        //Create 3 Container properties objects
        ContainerProperties containerProps1 = new ContainerProperties(Topology.DEMO_TOPIC);
        ContainerProperties containerProps2 = new ContainerProperties(Topology.DEMO_TOPIC);
        ContainerProperties containerProps3 = new ContainerProperties(Topology.DEMO_TOPIC);

        //Create 3 listeners.
        CustomMessageListener listener1 = new CustomMessageListener();
        CustomMessageListener listener2 = new CustomMessageListener();
        CustomMessageListener listener3 = new CustomMessageListener();

        //Set the listener for each container.
        containerProps1.setMessageListener(listener1);
        containerProps2.setMessageListener(listener2);
        containerProps3.setMessageListener(listener3);

        //Create 3 message listener containers with the 3 container props.
        KafkaMessageListenerContainer<Integer, String> container1 = createContainer(containerProps1);
        KafkaMessageListenerContainer<Integer, String> container2 = createContainer(containerProps2);
        KafkaMessageListenerContainer<Integer, String> container3 = createContainer(containerProps3);
        container1.setBeanName("testAuto1");
        container2.setBeanName("testAuto2");
        container3.setBeanName("testAuto3");

        container1.start();
        container2.start();
        container3.start();

        Thread.sleep(1000); // wait a bit for the container to start

        logger.info("Partitions "+container1.getAssignedPartitions().toString());

        //Verify that each container has 1 assigned partition.
        assertTrue(container1.getAssignedPartitions().size() == 1);
        assertTrue(container2.getAssignedPartitions().size() == 1);
        assertTrue(container3.getAssignedPartitions().size() == 1);

        KafkaTemplate<Integer, String> template = createTemplate();
        template.setDefaultTopic(Topology.DEMO_TOPIC);

        //Send 6 messages. 2 for each partition.
        template.sendDefault(0, 0, "first message");
        template.sendDefault(1, 1, "second message");
        template.sendDefault(2, 2, "third message");
        template.sendDefault(0, 3, "fourth message");
        template.sendDefault(1, 4, "fifth message");
        template.sendDefault(2, 5, "sixth message");

        template.flush();
        Thread.sleep(2000);
        assertTrue("Listener1 did not receive 2 messages.", listener1.getMessageCounter() == 2);
        assertTrue("Listener2 did not receive 2 messages.", listener2.getMessageCounter() == 2);
        assertTrue("Listener3 did not receive 2 messages.", listener3.getMessageCounter() == 2);

        container1.stop();
        container2.stop();
        container3.stop();

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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }


}

class CustomMessageListener implements MessageListener<Integer, String> {
    private int messageCounter = 0;
    Logger logger = LoggerFactory.getLogger(CustomMessageListener.class);

    @Override
    public void onMessage(ConsumerRecord<Integer, String> message) {
        logger.info("received: " + message);
        messageCounter++;
    }

    public int getMessageCounter() {
        return messageCounter;
    }

    public void setMessageCounter(int messageCounter) {
        this.messageCounter = messageCounter;
    }

}
