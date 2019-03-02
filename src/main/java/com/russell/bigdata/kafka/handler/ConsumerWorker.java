package com.russell.bigdata.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author liumenghao
 * @Date 2019/3/2
 */
@Slf4j
public class ConsumerWorker implements Runnable{

    private ConsumerRecord<String, String> consumerRecord;

    private ConsumerCallback callback;

    public ConsumerWorker(ConsumerRecord consumerRecord, ConsumerCallback consumerCallback){
        this.consumerRecord = consumerRecord;
        this.callback = consumerCallback;
    }


    @Override
    public void run() {
        String topic = consumerRecord.topic();
        String message = consumerRecord.value();
        Integer partition = consumerRecord.partition();
        Long offset = consumerRecord.offset();
        log.info("线程名称：{}, topic名称：{}, partition名称：{}, offset：{}", Thread.currentThread().getName(),
                topic, partition, offset);
        callback.callback(topic, message);
    }
}
