package com.russell.bigdata.kafka.handler;

/**
 * @author liumenghao
 * @Date 2019/2/28
 */
public interface ConsumerCallback {

    /**
     * kafka消费数据后的回调函数 ==> 消息处理逻辑
     *
     * @param topic
     * @param message
     */
    void callback(String topic, String message);
}
