package org.warpexchange_learning.push;

import io.vertx.core.Vertx;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.types.BulkType;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.redis.RedisCache;
import org.warpexchange_learning.common.support.LoggerSupport;

/**
 * 由于Servlet的线程池模型不能高效地支持成百上千的WebSocket长连接，所以我们需要基于Netty的Vert.x框架
 * PushService构建了一个Vert.x实例，并在该实例中创建Redis客户端并连接
 * 连接好redis后将对于来自redis的PUSH类型的响应进行解码并广播
 */
@Component
public class PushService extends LoggerSupport {

    @Value("${server.port}")
    private int serverPort;

    @Value("${exchange.config.hmac-key}")
    String hmacKey;

    @Value("${spring.redis.standalone.host:localhost}")
    private String redisHost;

    @Value("${spring.redis.standalone.port:6379}")
    private int redisPort;

    @Value("${spring.redis.standalone.password:}")
    private String redisPassword;

    @Value("${spring.redis.standalone.database:0}")
    private int redisDatabase = 0;

    private Vertx vertx;

    @PostConstruct
    public void startVertx() {
        logger.info("start vertx...");
        this.vertx = Vertx.vertx();

        var push = new PushVerticle(this.hmacKey, this.serverPort);
        vertx.deployVerticle(push);

        // 构造redis URL
        String url = "redis://" + (this.redisPassword.isEmpty() ? "" : ":" + this.redisPassword + "@") + this.redisHost + ":" + this.redisPort + "/" + this.redisDatabase;
        logger.info("create redis client: {}", url);
        Redis redis = Redis.createClient(vertx, url);

        redis.connect().onSuccess((conn) -> {
            logger.info("connect to redis ok.");
            // redis连接成功后，为连接设置一个处理器，用于接收 Redis 返回的响应，如果响应是PUSH类型，则调用broadcast广播响应中的消息
            conn.handler(response -> {
                if (response.type() == ResponseType.PUSH) {
                    int size = response.size();
                    if (size == 3) {
                        Response type = response.get(2);
                        if (type instanceof BulkType) {
                            String msg = type.toString();
                            if (logger.isDebugEnabled()) {
                                logger.debug("receive push message: {}", msg);
                            }
                            push.broadcast(msg);
                        }
                    }
                }
            });
            logger.info("try subscribe...");
            // 通过redis Sub订阅NOTIFICATION通道，这个订阅不需要回调函数，而是通过上面添加的handler监听来自redis的PUSH类型消息然后触发相应操作，即broadcast(msg)
            conn.send(Request.cmd(Command.SUBSCRIBE).arg(RedisCache.Topic.NOTIFICATION)).onSuccess(resp -> {
                logger.info("subscribe ok.");
            }).onFailure(err -> {
                logger.error("subscribe failed.", err);
                System.exit(1);
            });
        }).onFailure(err -> {
            logger.error("connect to redis failed.", err);
            System.exit(1);
        });
    }

    void exit(int exitCode) {
        this.vertx.close();
        System.exit(exitCode);
    }
}
