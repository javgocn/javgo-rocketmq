package cn.javgo.javgo.mq.rocketmq.quick_start;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Desc: 生产者
 *
 * @author javgo
 * @create 2024-08-06 19:27
 */
public class ProducerExample {

    private static final Logger logger  = LoggerFactory.getLogger(ProducerExample.class);

    /**
     * 接入点地址，也就是 Proxy 的 IP:Port
     */
    private static final String ENDPOINT = "localhost:8081";

    /**
     * 消息发送的目标 Topic，需要提前创建好
     */
    private static final String TOPIC = "TestTopic";

    public static void main(String[] args) {
        // 获取服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 构建客户端配置
        ClientConfiguration configuration = buildClientConfiguration(ENDPOINT);

        try (Producer producer = createProducer(provider, configuration, TOPIC)) {
            // 构建消息
            Message message = buildMessage(provider, TOPIC, "messageKey", "messageBody");
            // 发送消息，需要关注发送结果，并捕获失败等异常
            SendReceipt sendReceipt = producer.send(message);
            logger.info("消息发送成功，messageId={}", sendReceipt.getMessageId());
        } catch (IOException | ClientException e) {
            logger.error("消息发送失败。", e);
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
     * 创建并初始化生产者。
     *
     * @param provider 客户端服务提供者
     * @param configuration 客户端配置
     * @param topic 发送消息的目标主题
     * @return 初始化的生产者
     * @throws ClientException 如果生产者创建失败
     */
    private static Producer createProducer(ClientServiceProvider provider, ClientConfiguration configuration, String topic) throws ClientException {
        return provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();
    }

    /**
     * 构建要发送的消息。
     *
     * @param provider 客户端服务提供者
     * @param topic 消息发送的主题
     * @param key 消息的键
     * @param body 消息的主体内容
     * @return 构建的消息
     */
    private static Message buildMessage(ClientServiceProvider provider, String topic, String key, String body) {
        return provider.newMessageBuilder()
                .setTopic(topic)
                .setKeys(key)
                .setBody(body.getBytes())
                .build();
    }
}
