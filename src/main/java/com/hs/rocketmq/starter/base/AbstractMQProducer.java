package com.hs.rocketmq.starter.base;

import com.hs.rocketmq.starter.annotation.MQKey;
import com.hs.rocketmq.starter.annotation.MQProducer;
import com.hs.rocketmq.starter.exception.MQException;
import com.hs.rocketmq.starter.util.AnnotationHelper;
import com.hs.rocketmq.starter.util.SerilizerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.charset.Charset;

/**
 * Created by yipin on 2017/6/27.
 * RocketMQ的生产者的抽象基类
 */
@Slf4j
public abstract class AbstractMQProducer {

    private static final String[] DELAY_ARRAY = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h".split(" ");

    private static MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    public AbstractMQProducer() {
    }
    @Autowired
    private DefaultMQProducer producer;

    /**
     * 同步发送消息
     * @param builder  消息构建体
     * @throws MQException 消息异常
     */
    public void syncSend(MessageBuilder builder) throws MQException {
        Message message = null;
        try {
            message = buildMessage(builder);
            SendResult sendResult = producer.send(message);
            log.info("send rocketmq message ,messageId : {},topic : {}, tag :{}", sendResult.getMsgId(),message.getTopic(),message.getTags());
            this.doAfterSyncSend(message, sendResult);
        } catch (Exception e) {
            log.error("消息发送失败，topic : {},, tag :{}, msgObj {}", message.getTopic(),message.getTags(), message);
            throw new MQException("消息发送失败，topic :" + message.getTopic() + ",e:" + e.getMessage(),e);
        }
    }


    /**
     * 同步发送消息
     * @param message  消息体
     * @param hashKey  用于hash后选择queue的key
     * @throws MQException 消息异常
     */
    public void syncSendOrderly(Message message, String hashKey) throws MQException {
        /*if(StringUtils.isEmpty(hashKey)) {
            // fall back to normal
            syncSend(message);
        }
        try {
            SendResult sendResult = producer.send(message, messageQueueSelector, hashKey);
            log.debug("send rocketmq message orderly ,messageId : {}", sendResult.getMsgId());
            this.doAfterSyncSend(message, sendResult);
        } catch (Exception e) {
            log.error("顺序消息发送失败，topic : {}, msgObj {}", message.getTopic(), message);
            throw new MQException("顺序消息发送失败，topic :" + message.getTopic() + ",e:" + e.getMessage());
        }*/
    }

    /**
     * 重写此方法处理发送后的逻辑
     * @param message  发送消息体
     * @param sendResult  发送结果
     */
    public void doAfterSyncSend(Message message, SendResult sendResult) {}

    /**
     * 异步发送消息
     * @param builder msgObj
     * @param sendCallback 回调
     * @throws MQException 消息异常
     */
    public void asyncSend(MessageBuilder builder, SendCallback sendCallback) throws MQException {
        Message message = null;
        try {
            message = buildMessage(builder);
            producer.send(message, sendCallback);
            log.debug("send rocketmq message async");
        } catch (Exception e) {
            log.error("消息发送失败，topic : {}, msgObj {}", message.getTopic(), message);
            throw new MQException("消息发送失败，topic :" + message.getTopic() + ",e:" + e.getMessage(),e);
        }
    }

    private Message buildMessage(MessageBuilder builder)throws  MQException{
        Object messageBody = builder.getMessage();
        String topic =  builder.getTopic();
        String tag =  builder.getTag();
        Integer delayTimeLevel = builder.getDelayTimeLevel();
        byte[] body = null;
        String messageKey= "";
        try {
            Field[] fields = messageBody.getClass().getDeclaredFields();
            for (Field field : fields) {
                Annotation[] allFAnnos= field.getAnnotations();
                if(allFAnnos.length > 0) {
                    for (int i = 0; i < allFAnnos.length; i++) {
                        if(allFAnnos[i].annotationType().equals(MQKey.class)) {
                            field.setAccessible(true);
                            MQKey mqKey = MQKey.class.cast(allFAnnos[i]);
                            messageKey = StringUtils.isEmpty(mqKey.prefix()) ? field.get(messageBody).toString() : (mqKey.prefix() + field.get(messageBody).toString());
                        }
                    }
                }
            }
            Annotation[] producerAnnotations = this.getClass().getAnnotations();
            if(producerAnnotations.length > 0){
                String transmissionMode = null;
                for (Annotation ano:producerAnnotations ) {
                    if(ano.annotationType().equals(MQProducer.class)){
                        transmissionMode  = (String) AnnotationHelper.getAnnotationInfo(ano,"transmissionMode");
                        break;
                    }
                }
                if(StringUtils.isEmpty(transmissionMode)){
                    throw new RuntimeException("未设置值 transmissionMode(默认“JSON”模式)");
                }
                if(MessageExtConst.MESSAGE_TRANSMISSIONMODE_SERIALIZABLE.equals(transmissionMode)){
                    body = SerilizerUtil.writeToByteArray(messageBody);
                }else if(MessageExtConst.MESSAGE_TRANSMISSIONMODE_JSON.equals(transmissionMode)){
                    body = GsonMessageExt.getJson().toJson(messageBody).getBytes(Charset.forName("utf-8"));
                }
            }
        } catch (Exception e) {
            log.error("parse key error : {}" , e.getMessage());
        }

        if(StringUtils.isEmpty(topic)) {
            if(StringUtils.isEmpty(topic)) {
                throw new RuntimeException("no topic defined to send this message");
            }
        }
        Message message = new Message(topic, body);
        if (!StringUtils.isEmpty(tag)) {
            message.setTags(tag);
        }
        if(StringUtils.isNotEmpty(messageKey)) {
            message.setKeys(messageKey);
        }
        if(delayTimeLevel != null && delayTimeLevel > 0 && delayTimeLevel <= DELAY_ARRAY.length) {
            message.setDelayTimeLevel(delayTimeLevel);
        }
        return message;
    }



}
