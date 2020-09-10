package org.apache.dolphinscheduler.alert.utils;

/**
 * @Description TODO
 * @Author fang
 * @Date 2020-09-09 23:30
 **/

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMq通用管理工具类<br>
 * RabbitMQ是AMQP(高级消息队列协议)的标准实现
 * 1.单发送单(多)接收模式；<br>
 * 2.fanout发布订阅模式(fanout)；<br>
 * 3.routing路由模式(direct)；<br>
 * 4.topic通配符模式(topic)<br>
 * Broker:简单来说就是消息队列服务器实体;
 Channel:消息通道，在客户端的每个连接里，可建立多个channel，每个channel代表一个会话任务;
 Exchange:交换机，决定了消息路由规则，它指定消息按什么规则，路由到哪个队列;
 Queue:消息队列载体，每个消息都会被投入到一个或多个队列;
 Binding:绑定了Queue和Exchange，意即为符合什么样路由规则的消息，将会放置入哪一个消息队列;
 Routing Key:路由关键字，exchange根据这个关键字进行消息投递;
 vhost:虚拟主机，一个broker里可以开设多个vhost，用作不同用户的权限分离;
 producer:消息生产者，就是投递消息的程序;
 consumer:消息消费者，就是接受消息的程序;
 * 消息队列持久化包括3个部分:
 (1)exchange持久化，在声明时指定durable => 1
 (2)queue持久化，在声明时指定durable => 1
 (3)消息持久化，在投递时指定delivery_mode => 2(1是非持久化)
 如果exchange和queue都是持久化的，那么它们之间的binding也是持久化的。如果exchange和queue两者之间有一个持久化，一个非持久化，就不允许建立绑定。
 * 1.将交换机置为可持久;
 2.将通道置为可持久;
 3.消息发送时设置可持久;
 当我们"生产"了一条可持久化的消息，尝试中断MQ服务，启动消费者获取消息，消息依然能够恢复。相反，则抛出异常;
 * 消息队列的使用过程大概如下:
 (1)客户端连接到消息队列服务器，打开一个channel;
 (2)客户端声明一个exchange，并设置相关属性;
 (3)客户端声明一个queue，并设置相关属性;
 (4)客户端使用routing key，在exchange和queue之间建立好绑定关系;
 (5)客户端投递消息到exchange, exchange接收到消息后，就根据消息的key和已经设置的binding，进行消息路由，将消息投递到一个或多个队列里
 * exchange也有几个类型，
 完全根据key进行投递的叫做Direct交换机，例如，绑定时设置了routing key为"abc"，那么客户端提交的消息，只有设置了key为"abc"的才会投递到队列。
 对key进行模式匹配后进行投递的叫做Topic交换机，符号"#"匹配一个或多个词，符号"*"匹配正好一个词。例如"abc.#"匹配"abc.def.ghi"，"abc.*"只匹配"abc.def"。
 还有一种不需要key的，叫做Fanout交换机，它采取广播模式，一个消息进来时，投递到与该交换机绑定的所有队列。
 * Exchange
 Durability 持久性，这是exchange的可选属性，如果你Durability设置为false，那些当前会话结束的时候，该exchange也会被销毁;
 Auto delete 当没有队列或者其他exchange绑定到此exchange的时候，该exchange被销毁;
 Internal 表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定;
 无法声明2个名称相同 但是类型却不同的exchange;
 * Queue
 Durability 和exchange相同，未持久化的队列，服务重启后销毁;
 Auto delete 当没有消费者连接到该队列的时候，队列自动销毁;
 Exclusive 使队列成为私有队列，只有当前应用程序可用，当你需要限制队列只有一个消费者，这是很有用的;
 扩展属性如下对应源程序 RabbitMQ.Client.IModel.QueueDeclare(string, bool, bool, bool, System.Collections.Generic.IDictionary<string,object>)最后的参数,
 Message TTL 当一个消息被推送在该队列的时候 可以存在的时间 单位为ms，(对应扩展参数argument "x-message-ttl" );
 Auto expire 在队列自动删除之前可以保留多长时间(对应扩展参数argument "x-expires");
 Max length 一个队列可以容纳的已准备消息的数量(对应扩展参数argument "x-max-length");
 一旦创建了队列和交换机，就不能修改其标志了。例如，如果创建了一个non-durable的队列，然后想把它改变成durable的，唯一的办法就是删除这个队列然后重现创建;
 * RabbitMQ消息模型的核心理念是:发布者(producer)不会直接发送任何消息给队列。
 * 事实上，发布者(producer)甚至不知道消息是否已经被投递到队列。
 * 发布者(producer)只需要把消息发送给一个exchange。
 * exchange非常简单，它一边从发布者方接收消息，一边把消息推入队列。
 * exchange必须知道如何处理它接收到的消息，是应该推送到指定的队列还是是多个队列，或者是直接忽略消息。
 * 这些规则是通过exchange type来定义的;
 * @author
 */
public class RabbitMqPublisher {

    /**日志对象**/
    private final static Logger log = Logger.getLogger(RabbitMqPublisher.class);

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
