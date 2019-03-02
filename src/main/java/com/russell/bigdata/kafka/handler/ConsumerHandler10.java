package com.russell.bigdata.kafka.handler;

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

    private ConsumerCallback consumerCallback;

    /**
     * 构造函数
     *
     * @param brokerList
     * @param groupId
     * @param topic
     * @param callback
     */
    public ConsumerHandler10(String brokerList, String groupId, String topic, ConsumerCallback callback) {
        Properties props = createConsumerConfig(brokerList, groupId);
        this.consumerCallback = callback;
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    /**
     * 执行方法，开始消费
     *
     * @param workerNum 启动的线程数，一般对应topic的partition数量
     */
    public void execute(int workerNum) {
        executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (final ConsumerRecord<String, String> record : records) {
                executors.submit(new ConsumerWorker(record, consumerCallback));
            }

        }
    }

    /**
     * 用来关闭消费者，释放资源
     */
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

    /**
     * consumer的配置类
     *
     * @param kafkaBroker
     * @param groupId
     * @return
     */
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
