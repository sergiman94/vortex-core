package com.vortex.gremlin.api.exceptions;

public class ApiException  extends RuntimeException{
    private static final long serialVersionUID = 1L;
    private int statusCode;
    private String message;


    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ApiException(String message, int code){
        this.message=message;
        this.statusCode=code;
    }
}
