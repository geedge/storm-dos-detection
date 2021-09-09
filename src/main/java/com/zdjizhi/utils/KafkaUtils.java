package com.zdjizhi.utils;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import org.apache.kafka.clients.producer.*;

import java.util.List;
import java.util.Properties;

/**
 * NTC系统配置产生日志写入数据中心类
 *
 * @author Administrator
 * @create 2018-08-13 15:11
 */

public class KafkaUtils {

    private static final Log logger = LogFactory.get();

    /**
     * kafka生产者，用于向kafka中发送消息
     */
    private static Producer<String, String> kafkaProducer;

    /**
     * kafka生产者适配器（单例），用来代理kafka生产者发送消息
     */
    private static KafkaUtils kafkaUtils;

    private KafkaUtils() {
        initKafkaProducer();
    }

    public static KafkaUtils getInstance() {
        if (kafkaUtils == null) {
            kafkaUtils = new KafkaUtils();
        }
        return kafkaUtils;
    }

    public void sendMessage(List<String> list,String topic) {
        for (String value : list) {
            kafkaProducer.send(new ProducerRecord<>(topic, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error("写入" + topic + "出现异常", exception);
                    }
                }
            });
        }
        logger.debug("Log sent to National Center successfully!!!!!");
    }

    /**
     * 根据kafka生产者配置信息初始化kafka消息生产者,只初始化一次
     */
    private void initKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", CommonConfig.KAFKA_OUTPUT_BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", CommonConfig.PRODUCER_ACK);
        properties.put("retries", CommonConfig.RETRIES);
        properties.put("linger.ms", CommonConfig.LINGER_MS);
        properties.put("request.timeout.ms", CommonConfig.REQUEST_TIMEOUT_MS);
        properties.put("batch.size", CommonConfig.BATCH_SIZE);
        properties.put("buffer.memory", CommonConfig.BUFFER_MEMORY);
        properties.put("max.request.size", CommonConfig.MAX_REQUEST_SIZE);
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+CommonConfig.SASL_JAAS_CONFIG_USER+"\" password=\""+CommonConfig.SASL_JAAS_CONFIG_PASSWORD+"\";");


        kafkaProducer = new KafkaProducer<>(properties);
    }

}
