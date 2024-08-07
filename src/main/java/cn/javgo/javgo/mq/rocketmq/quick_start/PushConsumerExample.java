package cn.javgo.javgo.mq.rocketmq.quick_start;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * Desc: 消费者
 * <p>
 * Apache RocketMQ 支持 SimpleConsumer 和 PushConsumer 两种消费者类型，二者的区别在于 SimpleConsumer 是同步调用，
 * PushConsumer 是异步回调。
 *
 * @author javgo
 * @create 2024-08-06 19:37
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE) // 禁止外部实例化
public class PushConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(PushConsumerExample.class);

    /**
     * 接入点地址，也就是 Proxy 的 IP:Port
     */
    private static final String ENDPOINT = "localhost:8081";

    /**
     * 消费者分组
     */
    private static final String CONSUMER_GROUP = "YourConsumerGroup";

    /**
     * 订阅的目标 Topic
     */
    private static final String TOPIC = "TestTopic";

    /**
     * 订阅消息的过滤规则，表示订阅所有 Tag 的消息
     */
    private static final String TAG = "*";

    public static void main(String[] args) {
        // 获取服务提供者, 这里使用 final 关键字，禁止外部修改
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 创建客户端配置
        ClientConfiguration clientConfiguration = buildClientConfiguration(ENDPOINT);
        // 创建过滤表达式
        FilterExpression filterExpression = buildFilterExpression(TAG);

        try (PushConsumer pushConsumer = createPushConsumer(provider, clientConfiguration, CONSUMER_GROUP, TOPIC, filterExpression)) {
            // 阻塞主线程以保持消费者运行
            Thread.sleep(Long.MAX_VALUE);
        } catch (ClientException | IOException | InterruptedException e) {
            logger.error("PushConsumer error", e);
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
     * 构建过滤表达式。
     *
     * @param tag 过滤的标签
     * @return 过滤表达式
     */
    private static FilterExpression buildFilterExpression(String tag) {
        return new FilterExpression(tag, FilterExpressionType.TAG);
    }

    /**
     * 创建并初始化 PushConsumer。
     *
     * @param provider 客户端服务提供者
     * @param configuration 客户端配置
     * @param consumerGroup 消费者分组
     * @param topic 订阅的主题
     * @param filterExpression 过滤表达式
     * @return 初始化的 PushConsumer
     * @throws ClientException 如果消费者创建失败
     */
    private static PushConsumer createPushConsumer(ClientServiceProvider provider, ClientConfiguration configuration,
        String consumerGroup, String topic, FilterExpression filterExpression) throws ClientException {
        return provider.newPushConsumerBuilder()
                .setClientConfiguration(configuration)
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(messageView -> {
                    logger.info("Consume message successfully, messageId={}", messageView.getMessageId());
                    return ConsumeResult.SUCCESS;
                }).build();
    }
}
