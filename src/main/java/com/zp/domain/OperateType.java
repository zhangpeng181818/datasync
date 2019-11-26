package com.zp.domain;

public enum OperateType {

    READER("reader"),
    WRITER("writer"),
    ;

    private String value;

    OperateType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
