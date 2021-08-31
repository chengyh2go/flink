package com.sinobridge.flink.entity;

public class FissionGroupMember extends Fission {
    private Integer id;
    private String group_id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }
}
