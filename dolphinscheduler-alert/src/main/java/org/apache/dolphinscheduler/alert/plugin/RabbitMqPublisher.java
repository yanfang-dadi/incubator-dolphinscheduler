package org.apache.dolphinscheduler.alert.plugin;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqPublisher {

    /**日志对象**/
    private final static Logger log = LoggerFactory.getLogger(RabbitMqPublisher.class);

    /**RabbitMq连接工厂对象**/
    private volatile static ConnectionFactory factory = null;
    public static boolean stopRabbitFlag=false;

    /**构造方法**/
    public RabbitMqPublisher() {
        this(ConnectionFactory.DEFAULT_HOST,
                ConnectionFactory.DEFAULT_AMQP_PORT,
                ConnectionFactory.DEFAULT_VHOST,
                ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS);
    }

    /**
     * 构造方法
     * @param serverHost Rabbit服务主机
     */
    public RabbitMqPublisher(String serverHost) {
        this(serverHost, ConnectionFactory.DEFAULT_AMQP_PORT,
                ConnectionFactory.DEFAULT_VHOST,
                ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS);
    }

    /**
     * 构造方法
     * @param serverHost Rabbit服务主机
     * @param username 用户名
     * @param password 密码
     */
    public RabbitMqPublisher(String serverHost, String username, String password) {
        this(serverHost, ConnectionFactory.DEFAULT_AMQP_PORT,
                ConnectionFactory.DEFAULT_VHOST, username, password);
    }

    /**
     * 构造方法
     * @param serverHost Rabbit服务主机
     * @param vhost 虚拟host(类似权限组)
     * @param username 用户名
     * @param password 密码
     */
    public RabbitMqPublisher(String serverHost, String vhost, String username,
                       String password) {
        this(serverHost, ConnectionFactory.DEFAULT_AMQP_PORT, vhost, username,
                password);
    }

    /**
     * 构造方法
     * @param serverHost Rabbit服务主机
     * @param port Rabbit端口
     * @param username 用户名
     * @param password 密码
     */
    public RabbitMqPublisher(String serverHost, int port, String username,
                       String password) {
        this(serverHost, port, ConnectionFactory.DEFAULT_VHOST, username,
                password);
    }

    /**
     * 构造方法(初始化单例RabbitConnectionFactory)
     * @param serverHost Rabbit服务主机
     * @param port Rabbit端口
     * @param vhost 虚拟host(类似权限组)
     * @param username 用户名
     * @param password 密码
     */
    public RabbitMqPublisher(String serverHost, int port, String vhost,
                       String username, String password) {
        if (null == factory) {
            synchronized (ConnectionFactory.class) {
                if (null == factory) {
                    factory = new ConnectionFactory();
                    factory.setHost(serverHost);
                    factory.setPort(port);
                    factory.setVirtualHost(vhost);
                    factory.setUsername(username);
                    factory.setPassword(password);
                    log.info(">>>>>>Singleton ConnectionFactory Create Success>>>>>>");
                }
            }
        }
        if(stopRabbitFlag){
            stopRabbitFlag=false;
        }
    }

    /**
     * 创建连接
     * @return 连接
     * @throws Exception
     */
    private Connection buildConnection() throws Exception {
        return factory.newConnection();
    }

    /**
     * 创建信道
     * @param connection 连接
     * @return 信道
     * @throws Exception 运行时异常
     */
    private Channel buildChannel(Connection connection) throws Exception {
        return connection.createChannel();
    }

    /**
     * 关闭连接和信道
     * @param connection rabbitmq连接
     * @param channel rabbitmq信道
     */
    private void close(Connection connection, Channel channel) {
        try {
            if (null != channel) {
                channel.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (Exception e) {
            log.error(">>>>>>关闭RabbitMq的connection或channel发生异常>>>>>>", e);
        }
    }

    /**
     * 发送direct类型消息
     * @param exchangeName exchange名称
     * @param routingKey 路由key字符串
     * @param message 待发送的消息
     * @throws Exception 运行时异常
     */
    public void sendDirect(String exchangeName, String routingKey, String message) throws Exception {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = buildConnection();
            channel = buildChannel(connection);
            channel.basicPublish(exchangeName, routingKey,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            log.info("消息(" + message + "发布成功");
        } finally {
            close(connection, channel);
        }
    }

}
