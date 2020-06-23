package com.greyuhu.hbase.util;

/**
 * 对于返回的response进行统一的封装
 */
public class RestResponse {
    //    状态码
    private int code;
    //    提示消息
    private String message;
    //    携带信息
    private Object data;

    public RestResponse() {

    }

    /**
     * 请求成功无参数
     *
     * @return
     */
    public static RestResponse succuess() {
        RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(ResultCode.SUCCESS);

        return restResponse;
    }

    /**
     * 返回带有成功信息
     * @param message
     * @return
     */
    public static RestResponse succuess(String message) {
     RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(ResultCode.SUCCESS);
        restResponse.setMessage(message);
        return restResponse;
    }
    /**
     * 请求成功携带参数
     *
     * @param data
     * @return
     */
    public static RestResponse succuess(Object data) {
      RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(ResultCode.SUCCESS);
        restResponse.setData(data);
        return restResponse;
    }

    /**
     * 返回信息和参数的成功函数
     *
     * @param message
     * @param data
     * @return
     */
    public static RestResponse succuess(String message, Object data) {
       RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(ResultCode.SUCCESS);
        restResponse.setData(data);
        restResponse.setMessage(message);
        return restResponse;
    }

    /**
     * 请求失败
     *
     * @return
     */
    public static RestResponse fail() {
        RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(ResultCode.FAIL);

        return restResponse;
    }

    /**
     * 请求失败 定义失败原因
     *
     * @param resultCode
     * @return
     */
    public static RestResponse fail(ResultCode resultCode) {
     RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(resultCode);

        return restResponse;
    }

    /**
     * 请求失败，携带失败信息
     *
     * @param message
     * @return
     */
    public static RestResponse fail(String message) {
       RestResponse restResponse = new RestResponse();
        restResponse.setCode(ResultCode.FAIL.code());
        restResponse.setMessage(message);
        return restResponse;
    }

    /**
     * 请求失败，自定义错误代码与消息
     *
     * @param code
     * @param message
     * @return
     */
    public static RestResponse fail(Integer code, String message) {
     RestResponse restResponse = new RestResponse();
        restResponse.setCode(code);
        restResponse.setMessage(message);

        return restResponse;
    }

    /**
     * 请求失败，使用预定义的代码、携带参数
     *
     * @param resultCode
     * @param data
     * @return
     */
    public static RestResponse fail(ResultCode resultCode, Object data) {
        RestResponse restResponse = new RestResponse();
        restResponse.setResultCode(resultCode);
        restResponse.setData(data);

        return restResponse;
    }

    /**
     * 设置代码
     *
     * @param resultCode
     */
    private void setResultCode(ResultCode resultCode) {
        this.code = resultCode.code();
        this.message = resultCode.message();
    }

    /**
     * 设置参数
     *
     * @param data
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * 设置信息
     *
     * @param message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }


}
