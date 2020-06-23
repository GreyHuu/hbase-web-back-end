package com.greyuhu.hbase.util;


/**
 * API 统一返回状态码
 */
public enum ResultCode {
    /* 成功状态码 */
    SUCCESS(100, "请求成功"),
    FAIL(200, "请求失败"),
    TOKEN_INVALID(201, "token失效"),
    ACCESS_DENIED(202, "无权访问"),
    FAIL4DELETE(203, "删除失败"),
    FAIL4UPDATE(204, "更新失败");

    private final Integer code;
    private final String message;

    ResultCode(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer code() {
        return this.code;
    }

    public String message() {
        return this.message;
    }
}