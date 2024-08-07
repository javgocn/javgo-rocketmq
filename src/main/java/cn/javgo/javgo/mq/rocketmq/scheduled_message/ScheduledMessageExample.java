package cn.javgo.javgo.mq.rocketmq.scheduled_message;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.*;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * Desc: ScheduledMessageExample 展示了如何使用 RocketMQ 发送和消费定时/延时消息，
 * 包括使用 Producer 发送消息和使用 PushConsumer 及 SimpleConsumer 消费消息。
 * <p>
 * Apache RocketMQ 支持 SimpleConsumer 和 PushConsumer 两种消费者类型，二者的区别在于 SimpleConsumer 是同步调用，
 * PushConsumer 是异步回调。
 * </p>
 *
 * @author javgo
 * @create 2024-08-07 15:48
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE) // 禁止外部实例化
public class ScheduledMessageExample {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledMessageExample.class);

    /**
     * 接入点地址，也就是 Proxy 的 IP:Port
     */
    private static final String ENDPOINT = "localhost:8081";

    /**
     * 消息所属的 Topic
     */
    private static final String TOPIC = "TestTopic";

    /**
     * 消费者所属的 Group
     */
    private static final String CONSUMER_GROUP = "YourConsumerGroup";

    /**
     * 消息标签
     */
    private static final String TAG = "*";

    public static void main(String[] args) {
        // 创建客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        // 构建消息
        MessageBuilderImpl messageBuilder = new MessageBuilderImpl();
        // 以下示例表示：延迟时间为10分钟之后的Unix时间戳
        Long deliverTimeStamp = System.currentTimeMillis() + 10L * 60 * 1000;
        Message message = messageBuilder.setTopic(TOPIC) // 设置主题
                .setKeys("messageKey") // 设置消息索引键，可根据关键字精确查找某条消息
                .setTag(TAG) // 设置消息Tag，用于消费端根据指定Tag过滤消息
                .setDeliveryTimestamp(deliverTimeStamp) // 设置延时消息的投递时间戳
                .setBody("messageBody".getBytes()) // 消息体
                .build();

        // 初始化客户端配置
        ClientConfiguration clientConfiguration = buildClientConfiguration(ENDPOINT);

        try (Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(TOPIC)
                .build()) {

            // 发送定时/延时消息
            sendMessage(producer, message);

            // 消费示例一：使用 PushConsumer 消费定时消息
            consumeMessageWithPushConsumer(provider, clientConfiguration);

            // 消费示例二：使用 SimpleConsumer 消费定时消息
            consumeMessageWithSimpleConsumer(provider, clientConfiguration);
        } catch (ClientException | IOException e) {
            logger.error("Producer initialization failed.", e);
        }
    }

    /**
     * 构建客户端配置。
     *
     * @param endpoint RocketMQ 代理的端点
     * @return 客户端配置
     */
    private static ClientConfiguration buildClientConfiguration(String endpoint) {
        return ClientConfiguration.newBuilder()
                .setEndpoints(endpoint)
                .build();
    }

    /**
     * 发送消息
     *
     * @param producer 生产者实例
     * @param message  要发送的消息
     */
    private static void sendMessage(Producer producer, Message message) {
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常
            SendReceipt sendReceipt = producer.send(message);
            logger.info("Send message finished, messageId={}", sendReceipt.getMessageId());
        } catch (ClientException e) {
            logger.error("Send message failed.", e);
        }
    }

    /**
     * 使用 PushConsumer 消费定时消息
     *
     * @param provider            客户端服务提供者
     * @param clientConfiguration 客户端配置
     */
    private static void consumeMessageWithPushConsumer(ClientServiceProvider provider, ClientConfiguration clientConfiguration) {
        MessageListener messageListener = new MessageListener() {
            @Override
            public ConsumeResult consume(MessageView messageView) {
                logger.info("Receive message with delivery timestamp={}, messageId={}",
                        messageView.getDeliveryTimestamp(), messageView.getMessageId());
                // 根据消费结果返回状态
                return ConsumeResult.SUCCESS;
            }
        };

        // 创建过滤表达式
        FilterExpression filterExpression = new FilterExpression(TAG, FilterExpressionType.TAG);

        try (PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(CONSUMER_GROUP)
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC, filterExpression))
                .setMessageListener(messageListener)
                .build()) {

            // 阻塞主线程以保持消费者运行
            Thread.sleep(Long.MAX_VALUE);
        } catch (ClientException | IOException | InterruptedException e) {
            logger.error("PushConsumer error", e);
        }
    }

    /**
     * 使用 SimpleConsumer 消费定时消息
     *
     * @param provider            客户端服务提供者
     * @param clientConfiguration 客户端配置
     */
    private static void consumeMessageWithSimpleConsumer(ClientServiceProvider provider, ClientConfiguration clientConfiguration) {
        // 创建过滤表达式
        FilterExpression filterExpression = new FilterExpression(TAG, FilterExpressionType.TAG);

        try (SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(CONSUMER_GROUP)
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC, filterExpression))
                .build()) {

            while (true) {
                try {
                    // 主动获取消息进行消费处理并提交消费结果，每隔30秒拉取一次消息，每次拉取10条消息
                    List<MessageView> messageViewList = simpleConsumer.receive(10, Duration.ofSeconds(30));
                    messageViewList.forEach(messageView -> {
                        logger.info("Receive message, messageId={}", messageView.getMessageId());

                        // 消费处理完成后，需要主动调用 ACK 提交消费结果
                        try {
                            simpleConsumer.ack(messageView);
                        } catch (ClientException e) {
                            logger.error("Ack message failed.", e);
                        }
                    });
                } catch (ClientException e) {
                    // 如果遇到系统流控等原因造成拉取失败，需要重新发起获取消息请求
                    logger.error("Receive message failed.", e);
                }
            }
        } catch (ClientException | IOException e) {
            logger.error("SimpleConsumer error", e);
        }
    }
}
