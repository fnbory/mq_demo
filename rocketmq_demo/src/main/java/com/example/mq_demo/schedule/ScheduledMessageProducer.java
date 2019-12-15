package com.example.mq_demo.schedule;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * @Author: fnbory
 * @Date: 14/12/2019 下午 10:06
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");

        producer.setNamesrvAddr("47.100.213.16:9876");

        producer.start();
        int totalMessagesToSend = 100;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());

            message.setDelayTimeLevel(3);

            producer.send(message);
        }


        producer.shutdown();
    }
}
