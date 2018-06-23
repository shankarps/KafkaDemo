package com.kafkademo.util;

import com.kafkademo.Topology;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static com.kafkademo.Topology.DEMO_TOPIC;

@RunWith(SpringRunner.class)
@SpringBootTest
public class NonSpringKafkaConsumerTest {
    @Autowired
    private NonSpringKafkaConsumer nonSpringKafkaConsumer;

    @ClassRule
    public static KafkaEmbedded kafkaServer = new KafkaEmbedded(1, true, 1, DEMO_TOPIC);

    @Test
    public void testConsumePrintFromTopic(){
        nonSpringKafkaConsumer.consumePrintFromTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC);
    }

    @Test
    public void testConsumePrintFromTopicWithPollCount(){
        nonSpringKafkaConsumer.consumePrintFromTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC, 5);
    }

    @Test
    public void testConsumeMessagesFromTopicWithPollCount(){
        nonSpringKafkaConsumer.consumeMessagesFromTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC, 5,1);
    }

    @Test
    public void testSendMessagetoTopic(){
        nonSpringKafkaConsumer.sendMessagetoTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC, "This is a test message");
    }

    @Test
    public void testSendAndConsumeMessagetoTopic() throws InterruptedException {
        //Send 5 messages.
        for(int i=0; i < 5; i++){
            nonSpringKafkaConsumer.sendMessagetoTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC, "This is a test message "+i);
        }
        Thread.sleep(1000);

        //Verify if messages received.
        List<ConsumerRecord> records = nonSpringKafkaConsumer.consumeMessagesFromTopic(kafkaServer.getBrokersAsString(), DEMO_TOPIC, 5,1);
        //since we sent 5 messages, we will get at least 5 messages. There may be more in the topic.
        assert(records.size() >= 5);

    }

    @AfterClass
    public static void cleanup() throws Exception {
        kafkaServer.destroy();
    }
}
