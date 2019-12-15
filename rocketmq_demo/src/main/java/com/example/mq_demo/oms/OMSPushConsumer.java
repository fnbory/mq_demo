package com.example.mq_demo.oms;

import io.openmessaging.*;
import io.openmessaging.rocketmq.domain.NonStandardKeys;

/**
 * @Author: fnbory
 * @Date: 15/12/2019 下午 2:19
 */
public class OMSPushConsumer {
    public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint = MessagingAccessPointFactory
                .getMessagingAccessPoint("openmessaging:rocketmq://47.100.213.16:9876,47.100.213.16:9876/namespace");

        final PushConsumer consumer = messagingAccessPoint.
                createPushConsumer(OMS.newKeyValue().put(NonStandardKeys.CONSUMER_GROUP, "OMS_CONSUMER"));

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.shutdown();
                messagingAccessPoint.shutdown();
            }
        }));

        consumer.attachQueue("OMS_HELLO_TOPIC", new MessageListener() {
            @Override
            public void onMessage(final Message message, final ReceivedMessageContext context) {
                System.out.printf("Received one message: %s%n", message.headers().getString(MessageHeader.MESSAGE_ID));
                context.ack();
            }
        });

        consumer.startup();

    }
}
