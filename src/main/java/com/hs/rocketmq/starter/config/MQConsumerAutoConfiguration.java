package com.hs.rocketmq.starter.config;

import com.hs.rocketmq.starter.base.AbstractMQPushConsumer;
import com.hs.rocketmq.starter.model.StartConsumer;
import com.hs.rocketmq.starter.annotation.MQConsumer;
import com.hs.rocketmq.starter.base.MessageExtConst;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by suclogger on 2017/6/28.
 * 自动装配消息消费者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQConsumerAutoConfiguration extends MQBaseAutoConfiguration {

    // 维护一份map用于检测是否用同样的consumerGroup订阅了不同的topic+tag
    private Map<String, String> validConsumerMap;


    //注册开始已订阅数据
    private Map<String, StartConsumer> startConsumerMap;

    //注册订阅后,消费的监听器实现逻辑类
    private Map<String, AbstractMQPushConsumer> dealConsumerMap;


    @PostConstruct
    public void init() throws Exception {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQConsumer.class);

        validConsumerMap = new HashMap<String, String>();
        startConsumerMap = new HashMap<>();
        dealConsumerMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            publishConsumer(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String,StartConsumer> entry : startConsumerMap.entrySet()){
            String key = entry.getKey();
            StartConsumer startConsumer = entry.getValue();
            DefaultMQPushConsumer mqConsumer = startConsumer.getMqConsumer();

            if (MessageExtConst.CONSUME_MODE_CONCURRENTLY.equals(startConsumer.getConsumerModel())) {
                mqConsumer.registerMessageListener(new MessageListenerConcurrently() {
                   @Override
                   public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                       AbstractMQPushConsumer abstractMQPushConsumer = dealConsumerMap.get(key+"-"+context.getMessageQueue().getTopic());
                       return abstractMQPushConsumer.dealMessage(msgs,context);
                   }
               });
            } else if (MessageExtConst.CONSUME_MODE_ORDERLY.equals(startConsumer.getConsumerModel())) {
                mqConsumer.registerMessageListener(new MessageListenerOrderly() {
                   @Override
                   public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                       AbstractMQPushConsumer abstractMQPushConsumer = dealConsumerMap.get(key+"-"+context.getMessageQueue().getTopic());
                       return abstractMQPushConsumer.dealMessage(msgs,context);
                   }
               });
            } else {
                throw new RuntimeException("unknown consume mode ! only support CONCURRENTLY and ORDERLY");
            }
            mqConsumer.start();
        }
        // 清空map，等待回收
//        startConsumerMap = null;
        validConsumerMap = null;
    }

    private void publishConsumer(String beanName, Object bean) throws Exception {
        MQConsumer mqConsumer = applicationContext.findAnnotationOnBean(beanName, MQConsumer.class);
        if (StringUtils.isEmpty(mqProperties.getNameServerAddress())) {
            throw new RuntimeException("name server address must be defined");
        }
        Assert.notNull(mqConsumer.consumerGroup(), "consumer's consumerGroup must be defined");
        Assert.notNull(mqConsumer.topic(), "consumer's topic must be defined");
        if (!AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())) {
            throw new RuntimeException(bean.getClass().getName() + " - consumer未实现Consumer抽象类");
        }
        Environment environment = applicationContext.getEnvironment();

        String consumerGroup = environment.resolvePlaceholders(mqConsumer.consumerGroup());
        String topic = environment.resolvePlaceholders(mqConsumer.topic());
        String tags = "*";
        if(mqConsumer.tag().length == 1) {
            tags = environment.resolvePlaceholders(mqConsumer.tag()[0]);
        } else if(mqConsumer.tag().length > 1) {
            tags = StringUtils.join(mqConsumer.tag(), "||");
        }

        // 检查consumerGroup
        String key = consumerGroup+"-"+topic;
        if(!StringUtils.isEmpty(validConsumerMap.get(key))) {
            String exist = validConsumerMap.get(key);
            throw new RuntimeException("消费组重复订阅，请新增消费组用于新的group和topic组合: " + consumerGroup + "已经订阅了" + topic);
        } else {
            validConsumerMap.put(key, tags);
        }

        // 配置push consumer
        if (AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())) {
            StartConsumer startConsumer = null;
            DefaultMQPushConsumer mqPushConsumer = null;
            if(startConsumerMap.containsKey(consumerGroup)){
                startConsumer = startConsumerMap.get(consumerGroup);
                mqPushConsumer = startConsumer.getMqConsumer();
            }else{
                startConsumer = new StartConsumer();
                mqPushConsumer = new DefaultMQPushConsumer(consumerGroup);
                mqPushConsumer.setNamesrvAddr(mqProperties.getNameServerAddress());
                mqPushConsumer.setMessageModel(MessageModel.valueOf(mqConsumer.messageMode()));
                mqPushConsumer.setInstanceName(UUID.randomUUID().toString());
                mqPushConsumer.setVipChannelEnabled(mqProperties.getVipChannelEnabled());
                startConsumer.setMqConsumer(mqPushConsumer);
                startConsumer.setConsumerModel(mqConsumer.consumeMode());
                startConsumerMap.put(consumerGroup,startConsumer);
            }
            //订阅
            mqPushConsumer.subscribe(topic, tags);

            AbstractMQPushConsumer abstractMQPushConsumer = (AbstractMQPushConsumer) bean;
            abstractMQPushConsumer.setConsumer(mqPushConsumer);

            dealConsumerMap.put(key,abstractMQPushConsumer);
//            consumer.start();
        }

        log.info(String.format("%s is ready to subscribe message", bean.getClass().getName()));
    }
}