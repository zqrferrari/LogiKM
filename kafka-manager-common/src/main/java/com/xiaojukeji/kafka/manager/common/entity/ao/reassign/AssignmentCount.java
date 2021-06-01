package com.xiaojukeji.kafka.manager.common.entity.ao.reassign;

import com.xiaojukeji.kafka.manager.common.utils.ValidateUtils;

import java.util.ArrayList;
import java.util.List;

public class AssignmentCount {
    /**
     * 增加idx进行排序, 从而在多次调用的时候，减少堆积的情况的出现
     */
    private Integer idx;

    private Integer brokerId;

    private String rack;

    private Integer replicaNum;

    /**
     * 每个idx的副本的数量
     */
    private List<List<Integer>> assignList;

    public AssignmentCount(Integer idx, Integer brokerId, String rack, int replicaNum) {
        this.idx = idx;
        this.brokerId = brokerId;

        if (ValidateUtils.isBlank(rack)) {
            this.rack = "";
        } else {
            this.rack = rack;
        }

        this.replicaNum = replicaNum;

        this.assignList = new ArrayList<>();
        for (int i = 0; i < replicaNum; ++i) {
            this.assignList.add(new ArrayList<>());
        }
    }

    public Integer getIdx() {
        return idx;
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

    public List<List<Integer>> getAssignList() {
        return assignList;
    }

    @Override
    public String toString() {
        return "AssignmentCount{" +
                "idx=" + idx +
                ", brokerId=" + brokerId +
                ", rack='" + rack + '\'' +
                ", replicaNum=" + replicaNum +
                ", assignList=" + assignList +
                '}';
    }
}
