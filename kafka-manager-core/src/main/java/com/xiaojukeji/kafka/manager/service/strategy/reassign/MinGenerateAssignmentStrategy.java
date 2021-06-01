package com.xiaojukeji.kafka.manager.service.strategy.reassign;

import com.xiaojukeji.kafka.manager.common.entity.ao.reassign.AssignmentCount;
import com.xiaojukeji.kafka.manager.common.utils.JsonUtils;
import com.xiaojukeji.kafka.manager.common.utils.ValidateUtils;
import com.xiaojukeji.kafka.manager.common.zookeeper.znode.ReassignmentElemData;
import com.xiaojukeji.kafka.manager.common.zookeeper.znode.ReassignmentJsonData;
import com.xiaojukeji.kafka.manager.common.zookeeper.znode.brokers.BrokerMetadata;
import com.xiaojukeji.kafka.manager.common.zookeeper.znode.brokers.PartitionMap;
import com.xiaojukeji.kafka.manager.common.zookeeper.znode.brokers.TopicMetadata;
import com.xiaojukeji.kafka.manager.service.cache.PhysicalClusterMetadataManager;
import kafka.admin.AdminOperationException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 生成最小迁移计划
 * 生成最小迁移计划的前提是要满足如下两个要求：
 * 1. 保证满足跨rack分布的要求
 * 2. 分布均衡
 */
public abstract class MinGenerateAssignmentStrategy {
    /**
     * 生成迁移计划
     * @param clusterId 集群ID
     * @param topicName Topic名称
     * @param partitionIdList 迁移的分区ID
     * @param brokerIdSet 目标brokerId集合
     * @param minRackNumPerPartition 同个分区的副本, 必须最小跨越的rack数
     * @return 迁移计划
     */
    public static String generateAssignment(Long clusterId,
                                            String topicName,
                                            List<Integer> partitionIdList,
                                            Set<Integer> brokerIdSet,
                                            int minRackNumPerPartition,
                                            boolean minFocus) {
        if (ValidateUtils.anyNull(clusterId, topicName, partitionIdList, brokerIdSet)) {
            throw new AdminOperationException("param illegal");
        }

        // 获取当前需要迁移的Topic在broker上的分布信息
        Map<Integer, AssignmentCount> originBrokerAssignCountMap = getOriginAssignCount(clusterId, topicName, partitionIdList);
        int replicaNum = originBrokerAssignCountMap.values().iterator().next().getReplicaNum();

        // 最小rack数不符合要求时, 调整为合适值
        if (minRackNumPerPartition < 0) {
            minRackNumPerPartition = 0;
        }
        if (minRackNumPerPartition > replicaNum) {
            minRackNumPerPartition = replicaNum;
        }

        // 生成新的迁移计划
        Map<Integer, List<Integer>> newAssignmentMap = generateMinAssignment(
                clusterId,
                replicaNum,
                partitionIdList.size(),
                brokerIdSet,
                originBrokerAssignCountMap,
                minRackNumPerPartition,
                minFocus
        );

        // 格式转换
        ReassignmentJsonData reassignmentJsonData = ReassignmentJsonData.newInstance();
        for (Map.Entry<Integer, List<Integer>> newAssignmentEntry: newAssignmentMap.entrySet()) {
            ReassignmentElemData reassignmentElemData = new ReassignmentElemData();
            reassignmentElemData.setTopic(topicName);
            reassignmentElemData.setPartition(newAssignmentEntry.getKey());
            reassignmentElemData.setReplicas(newAssignmentEntry.getValue());
            reassignmentJsonData.getPartitions().add(reassignmentElemData);
        }
        return JsonUtils.toJSONString(reassignmentJsonData);
    }

    /**
     * 生成新的迁移计划
     * @param clusterId 集群ID
     * @param replicaNum 副本数
     * @param partitionNum 分区数
     * @param brokerIdSet 目标broker集合
     * @param originBrokerAssignCountMap 原分配统计信息
     * @param minRackNumPerPartition 同个分区的副本, 必须最小跨越的rack数
     * @return 迁移计划
     */
    private static Map<Integer, List<Integer>> generateMinAssignment(Long clusterId,
                                                                     Integer replicaNum,
                                                                     Integer partitionNum,
                                                                     Set<Integer> brokerIdSet,
                                                                     Map<Integer, AssignmentCount> originBrokerAssignCountMap,
                                                                     Integer minRackNumPerPartition,
                                                                     boolean minFocus) {
        Map<Integer, AssignmentCount> destBrokerAssignCountMap = initDestAssignCountMap(clusterId, brokerIdSet, replicaNum); // 初始化目标broker的分区分布统计信息

        // 每个broker针对每个位置的副本, 最多允许有多少副本
        int maxReplicaNum = partitionNum / destBrokerAssignCountMap.size();
        if (partitionNum % destBrokerAssignCountMap.size() != 0 && minFocus) {
            // 非整除, 同时强制最小迁移, 则尽量保证不迁移
            maxReplicaNum += 1;
        }

        // 最终的迁移计划
        Map<Integer, List<Integer>> newAssignmentMap = new HashMap<>();

        // 遍历每个位置的副本, 第0位是优先leader, 第1位是第一个follower, ......
        for (int idx = 0; idx < replicaNum; ++idx) {

            // 无需移动副本, 先保证无需移动的, 都分配好了, 后续就不会和需要迁移的冲突了
            for (Map.Entry<Integer, AssignmentCount> srcEntry: originBrokerAssignCountMap.entrySet()) {
                AssignmentCount destMinAssignCount = destBrokerAssignCountMap.get(srcEntry.getKey());
                if (ValidateUtils.isNull(destMinAssignCount)) {
                    // 当前Broker上的分区需要全部迁移走
                    continue;
                }

                for (Integer partitionId: srcEntry.getValue().getAssignList().get(idx)) {
                    if (destMinAssignCount.getAssignList().get(idx).size() >= maxReplicaNum) {
                        // 已经满足分配均衡的需求了, 则该broker不会再加分区
                        break;
                    }
                    // 检查当前分区分配到该broker是否合理, 合理直接加入
                    checkAndAssignReplica(idx, replicaNum, partitionId, minRackNumPerPartition, srcEntry.getKey(), newAssignmentMap, destBrokerAssignCountMap);
                }
                // 移除已经分配的分区, 遗留的都需要进行迁移
                srcEntry.getValue().getAssignList().get(idx).removeAll(destMinAssignCount.getAssignList().get(idx));
            }

            // 针对当前位置的副本的分布情况, 对分配统计进行排序, 分配少的broker排在前面, 优先被使用
            final Integer replicaIdx = idx;
            PriorityQueue<AssignmentCount> priorityQueue = new PriorityQueue<>(new Comparator<AssignmentCount>() {
                @Override
                public int compare(AssignmentCount o1, AssignmentCount o2) {
                    if (o1.getBrokerId().equals(o2.getBrokerId())) {
                        return 0;
                    }
                    if (o1.getAssignList().get(replicaIdx).size() < o2.getAssignList().get(replicaIdx).size()) {
                        return -1;
                    } else if (o1.getAssignList().get(replicaIdx).size() > o2.getAssignList().get(replicaIdx).size()) {
                        return 1;
                    }
                    if (o1.getIdx().equals(o2.getIdx())) {
                        // 如果随机数还一致, 则直接使用brokerId
                        return o1.getBrokerId() - o2.getBrokerId();
                    }
                    return o1.getIdx() - o2.getIdx();
                }
            });
            priorityQueue.addAll(destBrokerAssignCountMap.values());

            // 需要移动的副本
            for (AssignmentCount originMinAssignCount: originBrokerAssignCountMap.values()) {
                for (Integer partitionId: originMinAssignCount.getAssignList().get(idx)) {
                    boolean status = false;

                    // 遍历broker, 寻找最合适的broker, 找到则直接退出循环, 找不到时则抛出异常
                    Iterator<AssignmentCount> iterator = priorityQueue.iterator();
                    while (iterator.hasNext()) {
                        if (checkAndAssignReplica(idx, replicaNum, partitionId, minRackNumPerPartition, iterator.next().getBrokerId(), newAssignmentMap, destBrokerAssignCountMap)) {
                            // 检查之后可以使用
                            status = true;
                            break;
                        }
                    }
                    if (!status) {
                        throw new AdminOperationException("not all broker alive");
                    }
                }
            }
        }

        return newAssignmentMap;
    }

    private static boolean checkAndAssignReplica(Integer replicaIdx,
                                                 Integer replicaNum,
                                                 Integer partitionId,
                                                 Integer minRackNumPerPartition,
                                                 Integer brokerId,
                                                 Map<Integer, List<Integer>> newAssignmentMap,
                                                 Map<Integer, AssignmentCount> destAssignCountMap) {
        List<Integer> brokerIdList = newAssignmentMap.getOrDefault(partitionId, new ArrayList<>());

        Set<String> rackSet = destAssignCountMap.values().stream().filter(elem -> brokerIdList.contains(elem.getBrokerId()) || brokerId.equals(elem.getBrokerId())).map(elem -> elem.getRack()).collect(Collectors.toSet());
        if (replicaNum - brokerIdList.size() - 1 < minRackNumPerPartition - rackSet.size()) {
            // 加入之后, 剩余的broker数 < 还需要多少个副本才满足要求, 表示维持现状的话, 最小rack的要求就满足不了了, 因此必然需要进行迁移了
            return false;
        }

        destAssignCountMap.get(brokerId).getAssignList().get(replicaIdx).add(partitionId);

        brokerIdList.add(brokerId);
        newAssignmentMap.put(partitionId, brokerIdList);
        return true;
    }

    private static Map<Integer, AssignmentCount> initDestAssignCountMap(Long clusterId, Set<Integer> brokerIdSet, Integer replicaNum) {
        if (ValidateUtils.isEmptySet(brokerIdSet)) {
            return new HashMap<>();
        }

        Map<Integer, AssignmentCount> minAssignCountMap = new HashMap<>();

        Random random = new Random();
        for (Integer brokerId: brokerIdSet) {
            minAssignCountMap.put(brokerId, initMinAssignCount(clusterId, brokerId, random.nextInt(2013), true, replicaNum));
        }
        return minAssignCountMap;
    }

    private static Map<Integer, AssignmentCount> getOriginAssignCount(Long clusterId, String topicName, List<Integer> partitionIdList) {
        TopicMetadata topicMetadata = PhysicalClusterMetadataManager.getTopicMetadata(clusterId, topicName);
        if (ValidateUtils.isNull(topicMetadata)) {
            throw new AdminOperationException("topic not exist");
        }

        PartitionMap partitionMap = topicMetadata.getPartitionMap();
        if (ValidateUtils.isNull(partitionMap) || ValidateUtils.isEmptyMap(partitionMap.getPartitions())) {
            throw new AdminOperationException("topic metadata illegal");
        }

        Map<Integer, AssignmentCount> originAssignCountMap = new HashMap<>();
        for (Integer partitionId: partitionIdList) {
            List<Integer> brokerIdList = partitionMap.getPartitions().get(partitionId);
            if (ValidateUtils.isEmptyList(brokerIdList)) {
                throw new AdminOperationException("topic metadata illegal");
            }

            for (int idx = 0; idx < brokerIdList.size(); ++idx) {
                AssignmentCount minAssignCount = originAssignCountMap.get(brokerIdList.get(idx));
                if (ValidateUtils.isNull(minAssignCount)) {
                    originAssignCountMap.put(brokerIdList.get(idx), initMinAssignCount(clusterId, brokerIdList.get(idx), idx,false, brokerIdList.size()));
                    minAssignCount = originAssignCountMap.get(brokerIdList.get(idx));
                }

                minAssignCount.getAssignList().get(idx).add(partitionId);
            }
        }
        return originAssignCountMap;
    }

    private static AssignmentCount initMinAssignCount(Long clusterId, Integer brokerId, Integer idx, boolean focusAlive, Integer replicaNum) {
        BrokerMetadata brokerMetadata = PhysicalClusterMetadataManager.getBrokerMetadata(clusterId, brokerId);
        if (focusAlive && ValidateUtils.isNull(brokerMetadata)) {
            throw new AdminOperationException("not all broker alive");
        }
        if (ValidateUtils.isNull(brokerMetadata) || ValidateUtils.isBlank(brokerMetadata.getRack())) {
            return new AssignmentCount(idx, brokerId, "", replicaNum);
        }
        return new AssignmentCount(idx, brokerId, brokerMetadata.getRack(), replicaNum);
    }
}
