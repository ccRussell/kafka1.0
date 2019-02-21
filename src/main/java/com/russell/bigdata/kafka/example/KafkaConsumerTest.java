package com.russell.bigdata.kafka.example;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

import static com.russell.bigdata.kafka.common.Constants.KAFKA_BROKER;

/**
 * 测试使用，感受kafka java consumer api的使用方式
 *
 * @author liumenghao
 * @Date 2019/2/22
 */
@Slf4j
@Data
public class KafkaConsumerTest {

    private static String topic = "kafka_streaming_topic";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BROKER);
        props.put("group.id", "kafka_example");
        props.put("client.id", "kafka_example");
        props.put("fetch.max.bytes", 1024);

        // 自动提交偏移量
        props.put("enable.auto.commit", true);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                log.info("recodeOffset = " + record.offset() + " recodeValue = " + record.value());
            }
        }
    }
}
