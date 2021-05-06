/**
 * Copyright © 2016-2020 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.queue.processing;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Slf4j
public class BatchTbRuleEngineSubmitStrategy extends AbstractTbRuleEngineSubmitStrategy {

    private final int batchSize;
    /**
     * 切片中被处理成功消息的计数器，消息每处理成功一条计数器加1
     */
    private final AtomicInteger packIdx = new AtomicInteger(0);
    private final Map<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> pendingPack = new LinkedHashMap<>();
    private volatile BiConsumer<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer;

    public BatchTbRuleEngineSubmitStrategy(String queueName, int batchSize) {
        super(queueName);
        this.batchSize = batchSize;
    }

    @Override
    public void submitAttempt(BiConsumer<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer) {
        this.msgConsumer = msgConsumer;
        submitNext();
    }

    @Override
    public void update(ConcurrentMap<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> reprocessMap) {
        super.update(reprocessMap);
        packIdx.set(0);
    }

    /**
     * 切片pendingPack中的消息每当被处理成功一条，就删除一条，并将处理成功计数器加1
     * @param id
     */
    @Override
    protected void doOnSuccess(UUID id) {
        boolean endOfPendingPack;
        synchronized (pendingPack) {
            TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg> msg = pendingPack.remove(id);
            endOfPendingPack = msg != null && pendingPack.isEmpty();
        }
        if (endOfPendingPack) {
            packIdx.incrementAndGet();
            submitNext();
        }
    }

    private void submitNext() {
        //ex.100
        int listSize = orderedMsgList.size();
        //packIdx初始值为0，会在特定时期被重置为0。所以初始的切片开始下标startIdx=0
        //假设切片大小为50，而切片中被处理成功的消息量packIdx为25，则startIdx此时为100
        int startIdx = Math.min(packIdx.get() * batchSize, listSize);
        //batchSize=50
        int endIdx = Math.min(startIdx + batchSize, listSize);
        //从上可得出pendingPack的消息量为endIdx-startIdx
        //下面代码开始对消息进行分片，将startIdx-》endIdx范围内的消息装载到pendingPack中，pendingPack中存放的就是切好片的消息，且供msgConsumer进行处理
        synchronized (pendingPack) {
            pendingPack.clear();
            for (int i = startIdx; i < endIdx; i++) {
                IdMsgPair pair = orderedMsgList.get(i);
                pendingPack.put(pair.uuid, pair.msg);
            }
        }
        int submitSize = pendingPack.size();
        if (log.isDebugEnabled() && submitSize > 0) {
            log.debug("[{}] submitting [{}] messages to rule engine", queueName, submitSize);
        }
        //开始处理切片中的消息,此处的本质是：pendingPack.forEach((id, msg)->{})
        pendingPack.forEach(msgConsumer);

    }

}
