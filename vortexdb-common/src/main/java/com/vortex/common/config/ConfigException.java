package com.vortex.common.config;

/*
* This classs is used to throw a ConfigException either when the config doesn't exist or is not valid
* */

public class ConfigException extends RuntimeException {

    // TODO: REVIEW THIS UID
    private static final long serialVersionUID = - 8711375282196157058L;

    public ConfigException(String message) {
        super(message);
    }

    // TODO: research about Object... args
    public ConfigException(String message, Object... args){super(String.format(message,args));}

    // TODO: research about Throwable
    public ConfigException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }
}
