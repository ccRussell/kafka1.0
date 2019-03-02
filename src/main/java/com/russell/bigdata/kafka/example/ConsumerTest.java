package com.russell.bigdata.kafka.example;

import com.russell.bigdata.kafka.common.KafkaTopicType;
import com.russell.bigdata.kafka.handler.ConsumerHandler10;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static com.russell.bigdata.kafka.common.Constants.KAFKA_BROKER;

/**
 * 测试使用，感受kafka java consumer api的使用方式
 *
 * @author liumenghao
 * @Date 2019/2/22
 */
@Slf4j
@Data
public class ConsumerTest {

    public static void main(String[] args) throws Exception {
        String groupId = "kafka_example";
        init(groupId);
    }

    public static void init(String groupId) throws Exception {
        String topic0 = KafkaTopicType.THREE_PARTITION_TOPIC.getName();
        ConsumerHandler10 consumer = new ConsumerHandler10(KAFKA_BROKER, groupId, topic0,
                (topic, message) -> doProcessMessage(topic, message));
        consumer.execute(3);
    }

    public static void doProcessMessage(String topic, String message) {
        switch (topic) {
            case "kafka_partitions_topic": {
                log.info(message);
                break;
            }
            default: {
                log.info("topic 无效");
            }
        }
    }
}
