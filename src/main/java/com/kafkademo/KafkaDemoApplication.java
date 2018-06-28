package com.kafkademo;

import com.kafkademo.util.NonSpringKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.List;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(KafkaDemoApplication.class, args);
        NonSpringKafkaConsumer kafkaConsumer = context.getBean(NonSpringKafkaConsumer.class);

        List<ConsumerRecord> list = kafkaConsumer.searchMatchingMessageFromTopic(args[0], args[1], "Search Str");

        //Search for messages that have the search string. Loop and print them.
        list.stream().forEach(s -> {
            System.out.println(s.offset());
            System.out.println(s.value());
            System.out.println(s.partition());
            System.out.println(s.timestamp());
            System.out.println(s.key());});

	}
}
