package com.trx.rocketmq.trx;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Consumer {

    private static final Map<MessageQueue,Long> offseTable = new HashMap<MessageQueue, Long>();
    public static void main(String[] args) throws Exception{

        DefaultMQPullConsumer  consumer = new DefaultMQPullConsumer("my_test_consumer_group");
        consumer.setNamesrvAddr("192.168.124.44:9876");
        consumer.start();
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("q-2-1");

        for(MessageQueue mq:mqs){

            SINGLE_MQ:while(true){

                  try{

                      long offset = consumer.fetchConsumeOffset(mq,true);
                      PullResult pullResult = consumer.pullBlockIfNotFound(mq,null,getMessageQueueOffset(mq),32);
                      if(null!=pullResult.getMsgFoundList()){

                          for(MessageExt messageExt:pullResult.getMsgFoundList()){

                              System.out.println(new String(messageExt.getBody()));
                              System.out.println(pullResult);
                              System.out.println(messageExt);

                          }
                      }

                      putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
                      switch (pullResult.getPullStatus()){

                          case FOUND:
                              break;
                          case NO_MATCHED_MSG:
                              break;
                          case NO_NEW_MSG:
                              break SINGLE_MQ;
                          case OFFSET_ILLEGAL:
                                break;
default:
    break ;

                      }


                  }catch (Exception e){

//                      e.printStackTrace();
                  }
            }
        }
        consumer.shutdown();
    }


    private static void putMessageQueueOffset(MessageQueue mq,long offset){offseTable.put(mq,offset);}

    private static long getMessageQueueOffset(MessageQueue mq){

        Long offset = offseTable.get(mq);
        if(offset!=null){

            return offset;
        }

        return 0;
    }










































}
