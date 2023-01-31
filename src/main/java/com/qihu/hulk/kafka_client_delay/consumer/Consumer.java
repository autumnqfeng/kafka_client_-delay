package com.qihu.hulk.kafka_client_delay.consumer;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.qihu.hulk.kafka_client_delay.message.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author qiufeng
 */
@Component
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaConsumer<Integer, String> consumer;

    @Value("${consumer.server}")
    private String server;
    @Value("${consumer.group}")
    private String group;
    @Value("${consumer.topic}")
    private String topic;
    @Value("${consumer.session.timeout.ms}")
    private String sessionTimeout;
    @Value("${consumer.heartbeat.interval.ms}")
    private String heartbeatInterval;
    @Value("${consumer.enable}")
    private Boolean enable;
    @Value("${consumer.count}")
    private Integer count;

    public void init() {
        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.server);
        //必须指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.group);
        //设置数据key和value的序列化处理类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeout);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, this.heartbeatInterval);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //创建消息者实例
        this.consumer = new KafkaConsumer<>(props);
    }

    @PostConstruct
    public void run() {
        if (!this.enable) {
            return;
        }

        //Common Thread Pool
        ExecutorService pool = new ThreadPoolExecutor(5, 50,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new ThreadFactoryBuilder().setNameFormat("Consumer-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < count; i++) {
            pool.submit(this::consumer);
        }
    }

    private void consumer() {
        this.init();
        //订阅topic的消息
        this.consumer.subscribe(Collections.singletonList(this.topic));
        //到服务器中读取记录
        while (true){
            ConsumerRecords<Integer, String> records = this.consumer.poll(1000);
            // 消费记录
            for (ConsumerRecord<Integer, String> record : records) {
                Message message = JSON.parseObject(record.value(), Message.class);
                long elapsedTime = System.currentTimeMillis() - message.getStartTime();

                if (record.key()%5000 == 0) {
                    logger.info("message(key: {}) receive from partition({}), offset({}) in {} ms",
                            record.key(),
                            record.partition(),
                            record.offset(),
                            elapsedTime);
                }
            }
        }
    }

}
