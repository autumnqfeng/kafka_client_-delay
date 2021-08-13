package com.qihu.hulk.kafka_client_delay.consumer;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.qihu.hulk.kafka_client_delay.message.Message;
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
import java.time.Duration;
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
    @Value("${consumer.enable}")
    private Boolean enable;

    public void init() {
        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", this.server);
        //必须指定消费者组
        props.put("group.id", this.group);
        //设置数据key和value的序列化处理类
        props.put("key.deserializer", IntegerDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        //创建消息者实例
        this.consumer = new KafkaConsumer<>(props);
    }

    @PostConstruct
    public void run() {
        if (!this.enable) {
            return;
        }
        this.init();

        //Common Thread Pool
        ExecutorService pool = new ThreadPoolExecutor(5, 50,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new ThreadFactoryBuilder().setNameFormat("Consumer-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());

        pool.submit(this::consumer);
    }

    private void consumer() {
        //订阅topic的消息
        this.consumer.subscribe(Collections.singletonList(this.topic));
        //到服务器中读取记录
        while (true){
            ConsumerRecords<Integer, String> records = this.consumer.poll(Duration.ofMillis(100));
            // 消费记录
            for (ConsumerRecord<Integer, String> record : records) {
                Message message = JSON.parseObject(record.value(), Message.class);
                long elapsedTime = System.currentTimeMillis() - message.getStartTime();

                logger.info("message(key: {}, value: {}) receive from partition({}), offset({}) in {} ms",
                        record.key(),
                        message.getMsg(),
                        record.partition(),
                        record.offset(),
                        elapsedTime);
            }
        }
    }

}
