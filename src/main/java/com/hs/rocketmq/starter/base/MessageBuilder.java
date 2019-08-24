package com.hs.rocketmq.starter.base;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class MessageBuilder {
    private String topic;
    private String tag;
    private String key;
    private Object message;
    private Integer delayTimeLevel;

    /**
     * 创建消息体
     * @param topic
     * @param tag
     * @param message
     */
    public MessageBuilder(String topic, String tag, Object message) {
        this.topic = topic;
        this.tag = tag;
        this.message = message;
    }

    public MessageBuilder(String topic, String tag, String key, Object message, Integer delayTimeLevel) {
        this.topic = topic;
        this.tag = tag;
        this.key = key;
        this.message = message;
        this.delayTimeLevel = delayTimeLevel;
    }
}
