package com.hs.rocketmq.starter.base;

import com.hs.rocketmq.starter.util.AnnotationHelper;
import com.hs.rocketmq.starter.util.SerilizerUtil;
import com.hs.rocketmq.starter.annotation.MQConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.util.Assert;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Description：RocketMQ消费抽象基类
 * Created by Jay Chang on 2017/9/14
 * Modified By：
 */
@Slf4j
public abstract class AbstractMQConsumer<T> {

    /**
     * 反序列化解析消息
     *
     * @param message  消息体
     * @return 序列化结果
     */
    protected T parseMessage(MessageExt message) {
        if (message == null || message.getBody() == null) {
            return null;
        }
        final Type type = this.getMessageType();
        byte[] body = message.getBody();
        if (type instanceof Class) {
            try {
                Annotation[] consumerAnnotations = this.getClass().getAnnotations();
                if(consumerAnnotations.length > 0){
                    String transmissionMode = null;
                    for (Annotation ano:consumerAnnotations ) {
                        if(ano.annotationType().equals(MQConsumer.class)){
                            transmissionMode  = (String) AnnotationHelper.getAnnotationInfo(ano,"transmissionMode");
                            break;
                        }
                    }
                    if(StringUtils.isEmpty(transmissionMode)){
                        throw new RuntimeException("未设置值 transmissionMode(默认“JSON”模式)");
                    }
                    if(MessageExtConst.MESSAGE_TRANSMISSIONMODE_SERIALIZABLE.equals(transmissionMode)){

                        return (T) SerilizerUtil.readFromByteArray(body);
                    }else if(MessageExtConst.MESSAGE_TRANSMISSIONMODE_JSON.equals(transmissionMode)){
                        return GsonMessageExt.getJson().fromJson(new String(message.getBody()), type);
                    }
                }
            }catch (Exception e){
                log.error("parse message fail : {}", e.getMessage());
                e.printStackTrace();
            }
        } else {
            log.warn("Parse msg error. {}", message);
        }
        return null;
    }

    protected Map<String, Object> parseExtParam(MessageExt message) {
        Map<String, Object> extMap = new HashMap<String, Object>();

        // parse message property
        extMap.put(MessageExtConst.PROPERTY_TOPIC, message.getTopic());
        extMap.putAll(message.getProperties());
        // parse messageExt property
        extMap.put(MessageExtConst.PROPERTY_EXT_BORN_HOST, message.getBornHost());
        extMap.put(MessageExtConst.PROPERTY_EXT_BORN_TIMESTAMP, message.getBornTimestamp());
        extMap.put(MessageExtConst.PROPERTY_EXT_COMMIT_LOG_OFFSET, message.getCommitLogOffset());
        extMap.put(MessageExtConst.PROPERTY_EXT_MSG_ID, message.getMsgId());
        extMap.put(MessageExtConst.PROPERTY_EXT_PREPARED_TRANSACTION_OFFSET, message.getPreparedTransactionOffset());
        extMap.put(MessageExtConst.PROPERTY_EXT_QUEUE_ID, message.getQueueId());
        extMap.put(MessageExtConst.PROPERTY_EXT_QUEUE_OFFSET, message.getQueueOffset());
        extMap.put(MessageExtConst.PROPERTY_EXT_RECONSUME_TIMES, message.getReconsumeTimes());
        extMap.put(MessageExtConst.PROPERTY_EXT_STORE_HOST, message.getStoreHost());
        extMap.put(MessageExtConst.PROPERTY_EXT_STORE_SIZE, message.getStoreSize());
        extMap.put(MessageExtConst.PROPERTY_EXT_STORE_TIMESTAMP, message.getStoreTimestamp());
        extMap.put(MessageExtConst.PROPERTY_EXT_SYS_FLAG, message.getSysFlag());
        extMap.put(MessageExtConst.PROPERTY_EXT_BODY_CRC, message.getBodyCRC());
        extMap.put(MessageExtConst.PROPERTY_EXT_BODY,message.getBody());
        return extMap;
    }

    /**
     * 解析消息类型
     *
     * @return 消息类型
     */
    protected Type getMessageType() {
        Type superType = this.getClass().getGenericSuperclass();
        if (superType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superType;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            Assert.isTrue(actualTypeArguments.length == 1, "Number of type arguments must be 1");
            return actualTypeArguments[0];
        } else {
            // 如果没有定义泛型，解析为Object
            return Object.class;
//            throw new RuntimeException("Unkown parameterized type.");
        }
    }
}
