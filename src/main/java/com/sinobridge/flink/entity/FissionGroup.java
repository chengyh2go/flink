package com.sinobridge.flink.entity;

public class FissionGroup extends Fission {

    private Long id;
    private String group_code;
    private Long offset_value;
    private Integer partition_value;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroup_code() {
        return group_code;
    }

    public void setGroup_code(String group_code) {
        this.group_code = group_code;
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
        return "FissionGroup{" +
                "id=" + id +
                ", group_code='" + group_code + '\'' +
                ", offset_value=" + offset_value +
                ", partition_value=" + partition_value +
                '}';
    }
}
