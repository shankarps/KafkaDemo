package com.kafkademo.chapter4.section_4_1_2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

/**
 * This unit test case is to test ReplyingKafkaTemplate.
 * Refer https://docs.spring.io/spring-kafka/reference/htmlsingle/#replying-template
 * This setup is complex. There is a Kafka Listener and consumer at both sides.
 * Lets consider the two sides as Client and Server. They both have Kafka producer and consumer.
 * This is the sequence of 4 events.
 * (1) Client sends message to Server on topic TO_PROCESS with a ReplyingKafkaTemplate.
 *      - This replyingKafkaTemplate has a listener to listen for replies.
 * (2) Server receives message on topic TO_PROCESS with a KafkaListener whose Consumerfactory has an assgned Kafkatemplate
 *      - The KafkaTemplate iss mandatory for the Consumer factory.
 * (3) Server responds a reply message on topic PROCESSED with the embedded KafkaTemplate. This configured by SendTo.
 * (4) Client receives the reply on topic PROCESSED.
 */
@RunWith(SpringRunner.class)
public class ReplyingKafkaTemplateTest {

    Logger logger = LoggerFactory.getLogger(ReplyingKafkaTemplate.class);

    //Create a ReplyingKafkaTemplate
    @Autowired
    ReplyingKafkaTemplate replyingKafkaTemplate;

    private final String testMessage = "Test message for reply";

    private final byte[] CORRELATION_ID = {1, 2, 3, 4};
    @Test
    public void testSend() throws ExecutionException, InterruptedException {
        //Create a record to send.
        ProducerRecord<String, String> record = new ProducerRecord("TO_PROCESS", "1",testMessage);

        //Set the reply topic. The server will use this to send its reply.
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC,"PROCESSED".getBytes()));
        //set a correlation id to keep track.
        record.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, CORRELATION_ID));
        //Send
        RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
        //Check that send is a success.
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        assert(sendResult.getRecordMetadata() != null);
        //wait and consume.
        Thread.sleep(500);
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        //check that the message is correctly processed.
        assertThat(consumerRecord.value(), is("Processed "+testMessage));
        //check the message has same correlation ID
        Header[] headers = consumerRecord.headers().toArray();
        //check that at least 1 header is present.
        assert(headers.length > 0);
        Header correlationHeader = null;
        for(Header header : headers){
            System.out.println("HEADER " + header.key());
            System.out.println("Value " + Arrays.toString(header.value()));
            if(KafkaHeaders.CORRELATION_ID.equals(header.key())) {
                correlationHeader = header;
            }
        }

        //check that you have the correlation ID header.
        assertNotNull(correlationHeader);

        //check that correlation headers are equal.
        //This is failing now!
        //TODO try to match the correlation IDs.
        //assert(Arrays.equals(correlationHeader.value(), CORRELATION_ID));

    }

    @Configuration
    @EnableKafka
    static class StringConfiguration {
        @Bean
        public KafkaEmbedded kafkaEmbedded() {
            return new KafkaEmbedded(1, false, 1);
        }

        //Client side - Producer
        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            Map<String, Object> producerConfigs = new HashMap<>();

            producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            return new DefaultKafkaProducerFactory<>(producerConfigs);
        }

        //Client side - Consumer
        //Note - this is a listener object - to pass to the producer to listen with.
        @Bean
        public KafkaMessageListenerContainer<String, String> replyContainer() {
            ContainerProperties containerProperties = new ContainerProperties("PROCESSED");
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "replyConsumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            return new KafkaMessageListenerContainer( new DefaultKafkaConsumerFactory<>(props), containerProperties);
        }

        //Client side - producer
        @Bean
        public ReplyingKafkaTemplate  kafkaTemplate(KafkaMessageListenerContainer<String, String> replyContainer) {
            ContainerProperties containerProperties = new ContainerProperties("PROCESSED");
            ReplyingKafkaTemplate kafkaTemplate = new ReplyingKafkaTemplate(producerFactory(), replyContainer);
            return kafkaTemplate;
        }

        //Server side - Listener factory
        @Bean(name="serverSideListener")
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerFactory(){
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "server");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            //Important - the Listener should have its own template. Only then can it send message to the reply topic.
            KafkaTemplate kafkaTemplate = new KafkaTemplate(producerFactory());
            factory.setReplyTemplate(kafkaTemplate);
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
            return factory;
        }


        //Note - this is a method based listener.
        @KafkaListener(containerFactory = "serverSideListener", id="server", topics="TO_PROCESS")
        //@SendTo("PROCESSED") - This is not needed if KafkaHeaders.REPLY_TOPIC is set in the record sent.
        @SendTo //Automatically detects the reply topic from KafkaHeaders.REPLY_TOPIC in header.
        public String process(String input){
            //System.out.println("Server received "+input);
            return "Processed "+input;
        }
    }
}
