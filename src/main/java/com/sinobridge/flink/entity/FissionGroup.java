package com.sinobridge.flink.entity;

public class FissionGroup extends Fission {
    private Long id;
    private String group_code;

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

    @Override
    public String toString() {
        return "FissionGroup{" +
                "id=" + id +
                ", group_code='" + group_code + '\'' +
                '}';
    }
}
