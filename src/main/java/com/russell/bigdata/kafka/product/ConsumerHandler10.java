package com.russell.bigdata.kafka.product;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author liumenghao
 * @Date 2019/3/2
 */
@Slf4j
public class ConsumerHandler10 {

    private final KafkaConsumer<String, String> consumer;

    private ExecutorService executors;

    private KafkaCallback kafkaCallback;

    public ConsumerHandler10(String brokerList, String groupId, String topic, KafkaCallback callback) {
        Properties props = createConsumerConfig(brokerList, groupId);
        this.kafkaCallback = callback;
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void execute(int workerNum) {
        executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (final ConsumerRecord<String, String> record : records) {
                executors.submit(new ConsumerWorker(record, kafkaCallback));
            }

        }
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        if (executors != null) {
            executors.shutdown();
        }
        try {
            if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("Timeout.... Ignore for this case");
            }
        } catch (InterruptedException ignored) {
            log.error("Other thread interrupted this shutdown, ignore for this case.");
            Thread.currentThread().interrupt();
        }
    }

    private Properties createConsumerConfig(String kafkaBroker, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
        props.put("group.id", groupId);

        // 自动提交偏移量
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }


}
