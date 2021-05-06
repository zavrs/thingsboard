/**
 * Copyright © 2016-2020 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.queue.common;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.TbQueueMsgMetadata;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueRequestTemplate;
import org.thingsboard.server.common.stats.MessagesStats;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Slf4j
public class DefaultTbQueueRequestTemplate<Request extends TbQueueMsg, Response extends TbQueueMsg> extends AbstractTbQueueTemplate
        implements TbQueueRequestTemplate<Request, Response> {

    private final TbQueueAdmin queueAdmin;
    private final TbQueueProducer<Request> requestTemplate;
    private final TbQueueConsumer<Response> responseTemplate;

    private final ConcurrentMap<UUID, DefaultTbQueueRequestTemplate.ResponseMetaData<Response>> pendingRequests;
    private final boolean internalExecutor;
    private final ExecutorService executor;
    private final long maxRequestTimeout;
    private final long maxPendingRequests;
    private final long pollInterval;
    private volatile long tickTs = 0L;
    private volatile long tickSize = 0L;
    private volatile boolean stopped = false;

    private MessagesStats messagesStats;

    @Builder
    public DefaultTbQueueRequestTemplate(TbQueueAdmin queueAdmin,
                                         TbQueueProducer<Request> requestTemplate,
                                         TbQueueConsumer<Response> responseTemplate,
                                         long maxRequestTimeout,
                                         long maxPendingRequests,
                                         long pollInterval,
                                         ExecutorService executor) {
        this.queueAdmin = queueAdmin;
        this.requestTemplate = requestTemplate;
        this.responseTemplate = responseTemplate;
        this.pendingRequests = new ConcurrentHashMap<>();
        this.maxRequestTimeout = maxRequestTimeout;
        this.maxPendingRequests = maxPendingRequests;
        this.pollInterval = pollInterval;
        if (executor != null) {
            internalExecutor = false;
            this.executor = executor;
        } else {
            internalExecutor = true;
            this.executor = Executors.newSingleThreadExecutor();
        }
    }

    /**
     * request请求对应的响应response会被存入一个response的队列，该方法将启动线程从响应队列中
     */
    @Override
    public void init() {
        queueAdmin.createTopicIfNotExists(responseTemplate.getTopic());
        this.requestTemplate.init();
        tickTs = System.currentTimeMillis();
        responseTemplate.subscribe();
        executor.submit(() -> {
            long nextCleanupMs = 0L;
            while (!stopped) {
                try {
                    List<Response> responses = responseTemplate.poll(pollInterval);
                    if (responses.size() > 0) {
                        log.trace("Polling responses completed, consumer records count [{}]", responses.size());
                    }
                    responses.forEach(response -> {
                        byte[] requestIdHeader = response.getHeaders().get(REQUEST_ID_HEADER);
                        UUID requestId;
                        if (requestIdHeader == null) {
                            log.error("[{}] Missing requestId in header and body", response);
                        } else {
                            requestId = bytesToUuid(requestIdHeader);
                            log.trace("[{}] Response received: {}", requestId, response);
                            ResponseMetaData<Response> expectedResponse = pendingRequests.remove(requestId);
                            if (expectedResponse == null) {
                                log.trace("[{}] Invalid or stale request", requestId);
                            } else {
                                expectedResponse.future.set(response);
                            }
                        }
                    });
                    responseTemplate.commit();
                    tickTs = System.currentTimeMillis();//00:00:10
                    tickSize = pendingRequests.size();
                    if (nextCleanupMs < tickTs) {
                        //cleanup;
                        pendingRequests.forEach((key, value) -> {
                            if (value.expTime < tickTs) {
                                ResponseMetaData<Response> staleRequest = pendingRequests.remove(key);
                                if (staleRequest != null) {
                                    log.trace("[{}] Request timeout detected, expTime [{}], tickTs [{}]", key, staleRequest.expTime, tickTs);
                                    staleRequest.future.setException(new TimeoutException());
                                }
                            }
                        });
                        nextCleanupMs = tickTs + maxRequestTimeout;
                    }
                } catch (Throwable e) {
                    log.warn("Failed to obtain responses from queue.", e);
                    try {
                        Thread.sleep(pollInterval);
                    } catch (InterruptedException e2) {
                        log.trace("Failed to wait until the server has capacity to handle new responses", e2);
                    }
                }
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;

        if (responseTemplate != null) {
            responseTemplate.unsubscribe();
        }

        if (requestTemplate != null) {
            requestTemplate.stop();
        }

        if (internalExecutor) {
            executor.shutdownNow();
        }
    }

    @Override
    public void setMessagesStats(MessagesStats messagesStats) {
        this.messagesStats = messagesStats;
    }

    /**
     * 完成验证设备连接的任务：
     * 1、将mqtt接收到的消息经过一定处理后发送到request对应的队列中，DefaultTbQueueResponseTemplate循环从该队列中消费request消息并处理生成对应的响应，并将响应存入对应的响应队列。该响应队列的消息又不断的被init方法消费。由此，send方法
     *    和init方法构成闭环，完成请求的应答。
     * 2、【DefaultTbQueueResponseTemplate循环从该队列中消费request消息并处理生成对应的响应】：request消息的处理实际上是交给了DefaultTransportApiService的handle方法
    */
    @Override
    public ListenableFuture<Response> send(Request request) {
        if (tickSize > maxPendingRequests) {
            return Futures.immediateFailedFuture(new RuntimeException("Pending request map is full!"));
        }
        UUID requestId = UUID.randomUUID();
        request.getHeaders().put(REQUEST_ID_HEADER, uuidToBytes(requestId));
        request.getHeaders().put(RESPONSE_TOPIC_HEADER, stringToBytes(responseTemplate.getTopic()));
        request.getHeaders().put(REQUEST_TIME, longToBytes(System.currentTimeMillis()));
        /**
         * future极为重要，他用于存放请求的响应数据，是异步的。并且被封装到了请求对应的响应元数据体中responseMetaData，同时被send方法给返回，而responseMetaData又被存入了pendingRequests中。
         * init方法在获取到了响应数据后，就会将数据存放future中了。所以init方法中的future和send方法外的future是同一个对象，当init为future设置数据时，send方法外的future的数据也会同时改变，就可供响应数据的再次处理了。
         */
        SettableFuture<Response> future = SettableFuture.create();


        ResponseMetaData<Response> responseMetaData = new ResponseMetaData<>(tickTs + maxRequestTimeout, future);
        pendingRequests.putIfAbsent(requestId, responseMetaData);
        log.trace("[{}] Sending request, key [{}], expTime [{}]", requestId, request.getKey(), responseMetaData.expTime);
        if (messagesStats != null) {
            messagesStats.incrementTotal();
        }
        /*
        将设备的连接消息发送到InMemoryStorage，requestTemplate就是一个TbQueueProducer对象
        主题名称：tb_transport.api.requests，响应消息的存放主题：tb_transport.api.responses.localhost
         */
        requestTemplate.send(TopicPartitionInfo.builder().topic(requestTemplate.getDefaultTopic()).build(), request, new TbQueueCallback() {
            @Override
            public void onSuccess(TbQueueMsgMetadata metadata) {
                if (messagesStats != null) {
                    messagesStats.incrementSuccessful();
                }
                log.trace("[{}] Request sent: {}", requestId, metadata);
            }

            @Override
            public void onFailure(Throwable t) {
                if (messagesStats != null) {
                    messagesStats.incrementFailed();
                }
                pendingRequests.remove(requestId);
                future.setException(t);
            }
        });
        return future;
    }

    private static class ResponseMetaData<T> {
        private final long expTime;
        private final SettableFuture<T> future;

        ResponseMetaData(long ts, SettableFuture<T> future) {
            this.expTime = ts;
            this.future = future;
        }
    }

}
