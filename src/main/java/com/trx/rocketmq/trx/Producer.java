package com.trx.rocketmq.trx;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Producer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("my_test_producer_group");
        producer.setNamesrvAddr("192.168.124.44:9876");
        producer.start();
        for (int i = 0; i <6 ; i++) {

            Message msg = new Message("q-2-1","TagA","2673",("RocketMQ"+String.format("%05d",i)).getBytes());
            SendResult sendResult = producer.send(msg);
            System.out.println(String.format("%05d",i)+":"+sendResult);
        }

        producer.shutdown();

    }
}
