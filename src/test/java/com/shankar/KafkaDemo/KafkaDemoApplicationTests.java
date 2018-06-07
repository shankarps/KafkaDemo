package com.shankar.KafkaDemo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.shankar.KafkaDemo.Topology.DEMO_TOPIC;
import static org.junit.Assert.assertNotNull;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDemoApplicationTests {

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 2,
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

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<String, String>(
                consumerProps);
        messageConsumer = consumerFactory.createConsumer();
        assertNotNull(messageConsumer);
        embeddedKafka.consumeFromAllEmbeddedTopics(messageConsumer);
    }

    @Test
    public void verifyKafkaSetup(){
        assert(embeddedKafka.getPartitionsPerTopic() == 2);
    }

	@Test
	public void verifySendReceiveMessage() {
        kafkaTemplate.send(DEMO_TOPIC, "Test message from JUnit");
        ConsumerRecord<String, String> received = KafkaTestUtils.getSingleRecord(messageConsumer,DEMO_TOPIC);
        hasValue("Test message from JUnit").matches(received);
	}

}
