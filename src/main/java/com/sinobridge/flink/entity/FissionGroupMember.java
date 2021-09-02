package com.sinobridge.flink.entity;

public class FissionGroupMember extends Fission {
    private Long id;
    private String group_id;

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

    @Override
    public String toString() {
        return "FissionGroupMember{" +
                "id=" + id +
                ", group_id='" + group_id + '\'' +
                '}';
    }
}
