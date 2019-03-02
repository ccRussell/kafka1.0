package com.russell.bigdata.kafka.product;

/**
 * @author liumenghao
 * @Date 2019/2/28
 */
public interface KafkaCallback {

    /**
     * kafka消费数据后的回调函数 ==> 处理逻辑
     *
     * @param topic
     * @param message
     */
    void callback(String topic, String message);
}
