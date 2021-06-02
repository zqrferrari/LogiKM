package com.xiaojukeji.kafka.manager.common.entity.ao.reassign;

import com.xiaojukeji.kafka.manager.common.constant.KafkaConstant;
import com.xiaojukeji.kafka.manager.common.utils.ValidateUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AssignmentCount {
    private Integer brokerId;

    private String rack;

    private Integer replicaNum;

    /**
     * 增加randomIdx用于排序, 从而在多次调用的时候，减少在某个brokerId堆积的情况的出现. 这里仅是一维的随机, 没有对每一个位置的replica进行随机
     */
    private Integer randomIdx;

    /**
     * 当前broker在副本的每个位置的分配列表
     */
    private List<List<Integer>> replicaIdxAssignList;

    /**
     * 当前broker分配的副本
     */
    private Set<Integer> assignSet;

    public AssignmentCount(Integer randomIdx, Integer brokerId, String rack, int replicaNum) {
        this.brokerId = brokerId;
        if (ValidateUtils.isBlank(rack)) {
            this.rack = KafkaConstant.WITHOUT_RACK_INFO_NAME;
        } else {
            this.rack = rack;
        }
        this.replicaNum = replicaNum;

        this.randomIdx = randomIdx;

        this.replicaIdxAssignList = new ArrayList<>();
        for (int i = 0; i < replicaNum; ++i) {
            this.replicaIdxAssignList.add(new ArrayList<>());
        }
        this.assignSet = new HashSet<>();
    }

    public Integer getRandomIdx() {
        return randomIdx;
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public String getRack() {
        return rack;
    }

    public Integer getReplicaNum() {
        return replicaNum;
    }

    public List<List<Integer>> getReplicaIdxAssignList() {
        return replicaIdxAssignList;
    }

    public Set<Integer> getAssignSet() {
        return assignSet;
    }

    @Override
    public String toString() {
        return "AssignmentCount{" +
                "brokerId=" + brokerId +
                ", rack='" + rack + '\'' +
                ", replicaNum=" + replicaNum +
                ", randomIdx=" + randomIdx +
                ", replicaIdxAssignList=" + replicaIdxAssignList +
                ", assignSet=" + assignSet +
                '}';
    }
}
