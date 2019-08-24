package com.hs.rocketmq.starter.model;

import lombok.Data;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;

/**
 * @Description:
 * @Author: HS
 * @Date: 2019/8/2 15:27
 */
@Data
public class StartConsumer {

    private String consumerModel;

    private DefaultMQPushConsumer mqConsumer;
}
