package com.russell.bigdata.kafka.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author liumenghao
 * @Date 2019/1/13
 */
public class KafkaMessageProducer {

    /**
     * 序列化方法
     */
    private static final String DEFAULT_SERIALIZER_CLASS = StringSerializer.class.getName();

    /**
     * kafka api中的生产者
     */
    private KafkaProducer kafkaProducer;


    /**
     * kafka生产者构造方法
     *
     * @param brokerList kafka节点信息
     */
    public KafkaMessageProducer(String brokerList) {

        Properties producerConfig = createProducerConfig(brokerList, DEFAULT_SERIALIZER_CLASS);

        kafkaProducer = new KafkaProducer(producerConfig);
    }

    /**
     * 发送消息到kafka
     *
     * @param topic 消息被发送到的topic
     * @param value 发送的消息的内容
     */
    public void sendMessage(String topic, String value) {
        ProducerRecord data = new ProducerRecord(topic, value);
        kafkaProducer.send(data);
    }

    /**
     * 关闭kafka,释放资源
     */
    public void close() {
        kafkaProducer.close();
    }

    /**
     * 创建kafka配置类
     *
     * @param brokerList
     * @param serializerClass
     * @return
     */
    private Properties createProducerConfig(String brokerList, String serializerClass) {
        Properties props = new Properties();
        // broker 列表
        props.put("bootstrap.servers", brokerList);

        // 设置对key序列化的类
        props.put("key.serializer", serializerClass);

        // 设置对value序列化的类
        props.put("value.serializer", serializerClass);

        /**
         * 0        不等待结果返回
         * 1        等待至少有一个服务器返回数据接收标识
         * -1或all   表示必须接受到所有的服务器返回标识，及同步写入
         */
        props.put("request.required.acks", "1");

        /**
         * 内部发送数据是异步还是同步
         * sync 同步，默认
         * async异步
         */
        props.put("producer.type", "async");
        return props;
    }

    public static void main(String[] args) {
        System.out.println(StringSerializer.class.getName());
    }
}
