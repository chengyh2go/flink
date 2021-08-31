package com.sinobridge.flink.entity;

public class FissionGroup extends Fission {
    private Integer id;
    private String group_code;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getGroup_code() {
        return group_code;
    }

    public void setGroup_code(String group_code) {
        this.group_code = group_code;
    }
}
