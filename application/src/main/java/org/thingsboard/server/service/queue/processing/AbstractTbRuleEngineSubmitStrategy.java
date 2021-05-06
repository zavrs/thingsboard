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

import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public abstract class AbstractTbRuleEngineSubmitStrategy implements TbRuleEngineSubmitStrategy {

    protected final String queueName;
    /**
     * 初始时，包含了从数据中心消费到的数据；
     * 经历过循环后包含的数据是切片中不同状态的数据
     */
    protected List<IdMsgPair> orderedMsgList;
    private volatile boolean stopped;

    public AbstractTbRuleEngineSubmitStrategy(String queueName) {
        this.queueName = queueName;
    }

    protected abstract void doOnSuccess(UUID id);

    @Override
    public void init(List<TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgs) {
        orderedMsgList = msgs.stream().map(msg -> new IdMsgPair(UUID.randomUUID(), msg)).collect(Collectors.toList());
    }

    /**
     * 注意，这里的getPendingMap不是将orderedMsgList的引用返回，而是将orderedMsgList的拷贝进行返回
     * @return
     */
    @Override
    public ConcurrentMap<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> getPendingMap() {
        return orderedMsgList.stream().collect(Collectors.toConcurrentMap(pair -> pair.uuid, pair -> pair.msg));
    }

    /**
     * 一个切片经过处理后，其中的数据有几个状态：处理成功、处理失败、处理超时
     * 根据处理策略的不同，reprocessMap中的数据状态也就不同：
     * SKIP_ALL_FAILURES：对切片中处理失败的数据忽略，直接进入下一切片的处理
     * RETRY_ALL：将切片中状态为成功失败超时的都再次处理
     * RETRY_FAILED：只对失败的数据再次处理
     * RETRY_TIMED_OUT：对超时的数据再次处理
     * RETRY_FAILED_AND_TIMED_OUT：对失败且超时的数据再次处理
     * @param reprocessMap
     */
    @Override
    public void update(ConcurrentMap<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> reprocessMap) {
        List<IdMsgPair> newOrderedMsgList = new ArrayList<>(reprocessMap.size());
        for (IdMsgPair pair : orderedMsgList) {
            if (reprocessMap.containsKey(pair.uuid)) {
                newOrderedMsgList.add(pair);
            }
        }
        orderedMsgList = newOrderedMsgList;
    }

    /**
     * 当一个IdMsgPair被处理完成后执行，id是IdMsgPair的key
     * @param id
     */
    @Override
    public void onSuccess(UUID id) {
        if (!stopped) {
            doOnSuccess(id);
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
