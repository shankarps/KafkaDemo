package com.kafkademo.stack;

import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;

import java.util.concurrent.CountDownLatch;

import static com.kafkademo.chapter3.SpringConfigSendReceiveMessage.DEMO_TOPIC;

/** Generic listener to listen to messages and Events.
 *
 */
class Listener {
    @KafkaListener(id = "foo", topics = DEMO_TOPIC)
    public void listen1(String foo) {
        //process your message here.
    }

    @EventListener()
    public void eventHandler(NonResponsiveConsumerEvent event) {
        //When Kafka server is down, NonResponsiveConsumerEvent error is caught here.
        System.out.println("CAUGHT the event "+ event);
    }
}
