package com.example.mq_demo.simplemq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Author: fnbory
 * @Date: 14/12/2019 下午 7:15
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("please_rename_unique_group_name");
        // 指定NameSerer地址，类似zookeeper
        producer.setNamesrvAddr("47.100.213.16:9876");
        //启动
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息实例同时指定 topic, tag ， message body.
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 此处同步发送，所以每个result都要等结果返回，可靠性很高，但是很慢，适用于重要邮件通知之类的场景
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        // 关闭priducer
        producer.shutdown();
    }
}
