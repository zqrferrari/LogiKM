package com.xiaojukeji.kafka.manager.common.zookeeper.znode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zengqiao
 * @date 20/1/15
 */
public class ReassignmentJsonData {
    public static final Integer REASSIGNMENT_JSON_DATA_VERSION = 1;

    private Integer version;

    private List<ReassignmentElemData> partitions;

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public List<ReassignmentElemData> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<ReassignmentElemData> partitions) {
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "ReassignmentJsonDTO{" +
                "version=" + version +
                ", partitions=" + partitions +
                '}';
    }

    public static ReassignmentJsonData newInstance() {
        ReassignmentJsonData reassignmentJsonData = new ReassignmentJsonData();
        reassignmentJsonData.setVersion(ReassignmentJsonData.REASSIGNMENT_JSON_DATA_VERSION);
        reassignmentJsonData.setPartitions(new ArrayList<>());
        return reassignmentJsonData;
    }
}