package com.hs.rocketmq.starter.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

/**
 * @Description:
 * @Author: HS
 * @Date: 2019/5/28 14:21
 */
@Slf4j
public class SimpleSendCallBackHandler implements SendCallback {
    @Override
    public void onSuccess(SendResult sendResult) {
        log.info("send async rocketmq message ,status{},messageId : {}",sendResult.getSendStatus(), sendResult.getMsgId());
    }

    @Override
    public void onException(Throwable throwable) {
        log.error("消息发送失败，原因: {}", throwable.getMessage());
        throwable.printStackTrace();
    }
}
