package cn.javgo.javgo.mq.rocketmq.transaction_message;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.*;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.apache.rocketmq.shaded.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Desc: TransactionMessageExample 展示了如何使用 RocketMQ 进行事务消息的发送和处理，
 * 包括模拟订单表查询服务和本地事务执行结果。
 * <p>
 * Apache RocketMQ 支持事务消息，可以确保消息和本地事务的一致性。
 * </p>
 *
 * @author javgo
 * @create 2024-08-07 19:38
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE) // 禁止外部实例化
public class TransactionMessageExample {

    private static final Logger logger = LoggerFactory.getLogger(TransactionMessageExample.class);

    /**
     * 接入点地址，也就是 Proxy 的 IP:Port
     */
    private static final String ENDPOINT = "localhost:8081";

    /**
     * 消息所属的 Topic
     */
    private static final String TOPIC = "TestTopic";

    /**
     * 用于唯一标识订单
     */
    private static final String ORDER_ID = "xxx";

    public static void main(String[] args) {
        // 创建客户端服务提供者
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 创建客户端配置
        ClientConfiguration clientConfiguration = buildClientConfiguration(ENDPOINT);

        // 创建事务生产者
        try (Producer producer = createTransactionProducer(provider, clientConfiguration)) {
            // 开启事务分支
            Transaction transaction = beginTransaction(producer);

            // 构建事务消息
            Message message = buildTransactionMessage();

            // 发送半事务消息
            SendReceipt sendReceipt = sendTransactionMessage(producer, transaction, message);

            // 执行本地事务，并确定本地事务结果
            handleLocalTransaction(transaction);
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
     * 创建事务生产者，并设置事务检查器。
     *
     * @param provider            客户端服务提供者
     * @param clientConfiguration 客户端配置
     * @return 事务生产者实例
     * @throws ClientException 如果创建事务生产者失败
     */
    private static Producer createTransactionProducer(ClientServiceProvider provider, ClientConfiguration clientConfiguration) throws ClientException {
        return provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTransactionChecker(messageView -> { // 设置事务检查器
                    // 获取订单ID
                    final String orderId = messageView.getProperties().get("OrderId");
                    if (StringUtils.isBlank(orderId)) {
                        // 错误的消息，直接返回 Rollback
                        return TransactionResolution.ROLLBACK;
                    }
                    // 判断订单是否存在
                    return checkOrderById(orderId) ? TransactionResolution.COMMIT : TransactionResolution.ROLLBACK;
                }).build();
    }

    /**
     * 模拟订单表查询服务，用来确认订单事务是否提交成功。
     *
     * @param orderId 订单ID
     * @return 订单是否存在
     */
    private static boolean checkOrderById(String orderId) {
        // 模拟订单表查询逻辑
        return true;
    }

    /**
     * 开启事务分支。
     *
     * @param producer 事务生产者实例
     * @return 事务实例
     * @throws ClientException 如果开启事务分支失败
     */
    private static Transaction beginTransaction(Producer producer) throws ClientException {
        try {
            return producer.beginTransaction(); // 开启事务分支
        } catch (ClientException e) {
            logger.error("Begin transaction failed.", e);
            throw e;
        }
    }

    /**
     * 构建事务消息。
     *
     * @return 构建的消息实例
     */
    private static Message buildTransactionMessage() {
        MessageBuilderImpl messageBuilder = new MessageBuilderImpl();
        return messageBuilder.setTopic(TOPIC)
                .setKeys("messageKey")
                .setTag("messageTag")
                .addProperty("OrderId", ORDER_ID) // 设置订单ID
                .setBody("messageBody".getBytes())
                .build();
    }

    /**
     * 发送半事务消息。
     *
     * @param producer    事务生产者实例
     * @param transaction 事务实例
     * @param message     要发送的消息
     * @return 发送回执
     * @throws ClientException 如果发送消息失败
     */
    private static SendReceipt sendTransactionMessage(Producer producer, Transaction transaction, Message message) throws ClientException {
        try {
            return producer.send(message, transaction);
        } catch (ClientException e) {
            logger.error("Send transaction message failed.", e);
            transaction.rollback(); // 回滚事务
            throw e;
        }
    }

    /**
     * 处理本地事务结果。
     *
     * @param transaction 事务实例
     */
    private static void handleLocalTransaction(Transaction transaction) {
        // 执行本地事务
        boolean localTransactionOk = doLocalTransaction();
        // 根据本地事务执行结果，确定事务提交还是回滚
        if (localTransactionOk) {
            try {
                transaction.commit(); // 提交事务
                logger.info("Local transaction committed.");
            } catch (ClientException e) {
                logger.error("Commit transaction failed.", e);
            }
        } else {
            try {
                transaction.rollback(); // 回滚事务
                logger.info("Local transaction rolled back.");
            } catch (ClientException e) {
                logger.error("Rollback transaction failed.", e);
            }
        }
    }

    /**
     * 模拟本地事务的执行结果。
     *
     * @return 本地事务是否执行成功
     */
    private static boolean doLocalTransaction() {
        // 模拟本地事务执行逻辑
        return true;
    }
}
