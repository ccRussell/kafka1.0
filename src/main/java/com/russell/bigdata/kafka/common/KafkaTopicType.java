package com.russell.bigdata.kafka.common;

import lombok.Getter;

/**
 * @author liumenghao
 * @Date 2019/3/1
 */
public enum  KafkaTopicType {

    ONE_PARTITION_TOPIC(1, "kafka_streaming_topic"),
    THREE_PARTITION_TOPIC(2, "kafka_partitions_topic");

    @Getter
    private int code;

    @Getter
    private String name;

    KafkaTopicType(int code, String name) {
        this.code = code;
        this.name = name;
    }
}
