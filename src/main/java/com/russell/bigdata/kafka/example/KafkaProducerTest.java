package com.russell.bigdata.kafka.example;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static com.russell.bigdata.kafka.common.Constants.KAFKA_BROKER;

/**
 * 测试使用，感受kafka java producer api的使用方式
 *
 * @author liumenghao
 * @Date 2019/2/22
 */
@Slf4j
@Data
public class KafkaProducerTest {

    private static final String TOPIC = "kafka_streaming_topic";

    private static int count;

    public static void main(String[] args) throws Exception {
        KafkaMessageProducer producer = new KafkaMessageProducer(KAFKA_BROKER);
        while (true) {
            Thread.sleep(1000);
            producer.sendMessage(TOPIC, "测试生产数据" + " " + count);
            log.info("测试生产数据" + " " + count);
            count++;
        }
    }
}
