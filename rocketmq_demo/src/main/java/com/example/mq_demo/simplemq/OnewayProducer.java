package com.example.mq_demo.simplemq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author: fnbory
 * @Date: 14/12/2019 下午 7:28
 */
public class OnewayProducer {
    public static void main(String[] args) throws Exception{

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.setNamesrvAddr("47.100.213.16:9876");

        producer.start();
        for (int i = 0; i < 100; i++) {

            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 这里没有回调，不是可靠的，即只管消息发过去，成不成不管，可以用来收集日志
            producer.sendOneway(msg);

        }

        producer.shutdown();
    }
}
