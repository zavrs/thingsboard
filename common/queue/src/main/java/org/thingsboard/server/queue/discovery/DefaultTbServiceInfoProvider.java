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
package org.thingsboard.server.queue.discovery;

/**
 * 对TB平台提供的服务信息进行初始化，并封装成一个ServiceInfo对象，一个服务具有唯一的serviceId，每个服务都默认绑定一个租户，
 * 能提供多种服务类型【TB_CORE：核心服务, TB_RULE_ENGINE：规则引擎, TB_TRANSPORT：信息传输服务, JS_EXECUTOR：js执行器;】
 * 如果提供了规则引擎服务且配置了规则引擎的配置，则加载其配置：
 * 1、规则引擎的消息主题是tb_rule_engine：每隔25毫秒从主题中poll一次消息
 * 2、主题tb_rule_engine下管理着三个子队列：Main-主队列，HighPriority-高优先级队列，SequentialByOriginator-按发起者排序队列
 * 3、每个子队列都对应有一个主题：Main-主队列：：tb_rule_engine.main，HighPriority-高优先级队列：：tb_rule_engine.hp，SequentialByOriginator-按发起者排序队列：：tb_rule_engine.sq
 * 4、每个子队列的主题都有各自的默认分区和消息的提交策略、处理策略
 *
 *
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.settings.TbQueueRuleEngineSettings;
import org.thingsboard.server.queue.settings.TbRuleEngineQueueConfiguration;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DefaultTbServiceInfoProvider implements TbServiceInfoProvider {

    @Getter
    @Value("${service.id:#{null}}")
    private String serviceId;

    @Getter
    @Value("${service.type:monolith}")
    private String serviceType;

    @Getter
    @Value("${service.tenant_id:}")
    private String tenantIdStr;

    @Autowired(required = false)
    private TbQueueRuleEngineSettings ruleEngineSettings;

    private List<ServiceType> serviceTypes;
    private ServiceInfo serviceInfo;
    private TenantId isolatedTenant;

    @PostConstruct
    public void init() {
        if (StringUtils.isEmpty(serviceId)) {
            try {
                serviceId = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                serviceId = org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(10);
            }
        }
        log.info("Current Service ID: {}", serviceId);
        if (serviceType.equalsIgnoreCase("monolith")) {
            serviceTypes = Collections.unmodifiableList(Arrays.asList(ServiceType.values()));
        } else {
            serviceTypes = Collections.singletonList(ServiceType.of(serviceType));
        }
        ServiceInfo.Builder builder = ServiceInfo.newBuilder()
                .setServiceId(serviceId)
                .addAllServiceTypes(serviceTypes.stream().map(ServiceType::name).collect(Collectors.toList()));
        UUID tenantId;
        if (!StringUtils.isEmpty(tenantIdStr)) {
            tenantId = UUID.fromString(tenantIdStr);
            isolatedTenant = new TenantId(tenantId);
        } else {
            tenantId = TenantId.NULL_UUID;
        }
        builder.setTenantIdMSB(tenantId.getMostSignificantBits());
        builder.setTenantIdLSB(tenantId.getLeastSignificantBits());

        if (serviceTypes.contains(ServiceType.TB_RULE_ENGINE) && ruleEngineSettings != null) {
            for (TbRuleEngineQueueConfiguration queue : ruleEngineSettings.getQueues()) {
                TransportProtos.QueueInfo queueInfo = TransportProtos.QueueInfo.newBuilder()
                        .setName(queue.getName())
                        .setTopic(queue.getTopic())
                        .setPartitions(queue.getPartitions()).build();
                builder.addRuleEngineQueues(queueInfo);
            }
        }

        serviceInfo = builder.build();
    }

    @Override
    public ServiceInfo getServiceInfo() {
        return serviceInfo;
    }

    @Override
    public boolean isService(ServiceType serviceType) {
        return serviceTypes.contains(serviceType);
    }

    @Override
    public Optional<TenantId> getIsolatedTenant() {
        return Optional.ofNullable(isolatedTenant);
    }
}
