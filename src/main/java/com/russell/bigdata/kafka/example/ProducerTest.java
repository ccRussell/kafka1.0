package com.russell.bigdata.kafka.example;

import com.russell.bigdata.kafka.common.KafkaTopicType;
import com.russell.bigdata.kafka.handler.ProducerHandler10;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import static com.russell.bigdata.kafka.common.Constants.KAFKA_BROKER;

/**
 * 测试使用，感受kafka java producer api的使用方式
 *
 * @author liumenghao
 * @Date 2019/2/22
 */
@Slf4j
@Data
public class ProducerTest {

    private static int count;

    public static void main(String[] args) throws Exception {
        ProducerHandler10 producer = new ProducerHandler10(KAFKA_BROKER);
        while (true) {
            Thread.sleep(1000);
            String topic = KafkaTopicType.THREE_PARTITION_TOPIC.getName();
            String message = "测试生产数据 " + count;
            producer.sendMessage(topic, message,
                    (metadata, exception) -> callback(metadata, exception));
            log.info("测试生产数据" + " " + count);
            count++;
        }
    }

    /**
     * 回调函数，当kafka发送数据完成后，会从服务端返回一个RecordMetadata
     *
     * @param metadata
     * @param exception
     */
    private static void callback(RecordMetadata metadata, Exception exception) {
        String topic = metadata.topic();
        Integer partition = metadata.partition();
        Long offset = metadata.offset();
        log.info("topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
