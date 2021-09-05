package com.sinobridge.flink.entity;

public class FissionGroupMember extends Fission {

    private Long id;
    private String group_id;
    private Long offset_value;
    private Integer partition_value;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public Long getOffset_value() {
        return offset_value;
    }

    public void setOffset_value(Long offset_value) {
        this.offset_value = offset_value;
    }

    public Integer getPartition_value() {
        return partition_value;
    }

    public void setPartition_value(Integer partition_value) {
        this.partition_value = partition_value;
    }

    @Override
    public String toString() {
        return "FissionGroupMember{" +
                "id=" + id +
                ", group_id='" + group_id + '\'' +
                ", offset_value=" + offset_value +
                ", partition_value=" + partition_value +
                '}';
    }
}
