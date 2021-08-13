package com.qihu.hulk.kafka_client_delay.message;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author qiufeng
 */
@Data
@AllArgsConstructor
public class Message {
    @JSONField(name = "no")
    private Integer no;
    @JSONField(name = "startTime")
    private Long startTime;
    @JSONField(name = "msg")
    private String msg;
}
