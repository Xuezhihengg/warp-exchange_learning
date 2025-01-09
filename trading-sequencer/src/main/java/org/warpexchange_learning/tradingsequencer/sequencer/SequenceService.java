package org.warpexchange_learning.tradingsequencer.sequencer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import org.warpexchange_learning.common.message.event.AbstractEvent;
import org.warpexchange_learning.common.messaging.*;
import org.warpexchange_learning.common.support.LoggerSupport;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SequenceService extends LoggerSupport implements CommonErrorHandler {

    private static final String GROUP_ID = "SequencerGroup";

    @Autowired
    private SequenceHandler sequenceHandler;

    @Autowired
    private MessagingFactory messagingFactory;

    @Autowired
    private MessageTypes messageTypes;

    private MessageProducer<AbstractEvent> messageProducer;

    // 全局唯一递增ID:
    private AtomicLong sequence;
    private Thread jobThread;
    private boolean running;

    @PostConstruct
    public void init() {
        Thread thread = new Thread(() -> {
            logger.info("start sequence job...");
            // 向交易引擎发送定好序的请求
            this.messageProducer = this.messagingFactory.createMessageProducer(Messaging.Topic.TRADE, AbstractEvent.class);
            // find max event id:
            this.sequence = new AtomicLong(this.sequenceHandler.getMaxSequenceId());

            // init consumer:
            logger.info("create message consumer for {}...", getClass().getName());
            // share same group id:
            // 接受来自API模块的消息，即创建订单请求
            MessageConsumer consumer = this.messagingFactory.createBatchMessageListener(Messaging.Topic.SEQUENCE, GROUP_ID, this::processMessages, this);
            // start running:
            this.running = true;
            // 这个忙等循环是为了保持线程不退出，让producer和consumer能一直发挥作用
            while (running) {
                try {
                    Thread.sleep(1000);
                }catch (InterruptedException e) {
                    break;
                }
            }
            // close message consumer:
            logger.info("close message consumer for {}...", getClass().getName());
            consumer.stop();
            System.exit(1);
        });
        this.jobThread = thread;
        this.jobThread.start();
    }

    @PreDestroy
    public void shutdown() {
        logger.info("shutdown sequence service...");
        running = false;
        if (jobThread != null) {
            jobThread.interrupt();
            try {
                jobThread.join(5000);
            } catch (InterruptedException e) {
                logger.error("interrupt job thread failed", e);
            }
            jobThread = null;
        }
    }

    private boolean crash = false;

    /**
     * Message consumer error handler
     */
    @Override
    public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
                            MessageListenerContainer container, Runnable invokeListener) {
        logger.error("batch error!", thrownException);
        panic();
    }

    private void sendMessages(List<AbstractEvent> messages) {
        this.messageProducer.sendMessages(messages);
    }

    // 接收消息并定序再发送:
    private synchronized void processMessages(List<AbstractEvent> messages) {
        if (!running || crash) {
            panic();
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("do sequence for {} messages...", messages.size());
        }
        long start = System.currentTimeMillis();
        // 定序后的事件消息:
        List<AbstractEvent> sequenced = null;
        try {
            sequenced = this.sequenceHandler.sequenceMessages(this.messageTypes, this.sequence, messages);
        } catch (Throwable e) {
            // 定序出错时进程退出:
            logger.error("exception when do sequence", e);
            shutdown();
            panic();
            throw new Error(e);
        }
        if (logger.isInfoEnabled()) {
            long end = System.currentTimeMillis();
            logger.info("sequenced {} messages in {} ms. current sequence id: {}", messages.size(), (end - start),
                    this.sequence.get());
        }
        // 将定序好的消息发给TRADE，也即交易引擎模块
        sendMessages(sequenced);
    }

    private void panic() {
        this.crash = true;
        this.running = false;
        System.exit(1);
    }

}
