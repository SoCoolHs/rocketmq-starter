package com.hs.rocketmq.starter.annotation;

import com.hs.rocketmq.starter.base.MessageExtConst;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * Created by yipin on 2017/6/27.
 * RocketMQ生产者自动装配注解
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MQProducer {

    /**
     * JSON模式传输： JSON
     * 序列化模式传输： Serializable
     * @return 消息模式
     * */
    String transmissionMode() default  MessageExtConst.MESSAGE_TRANSMISSIONMODE_JSON;
}
