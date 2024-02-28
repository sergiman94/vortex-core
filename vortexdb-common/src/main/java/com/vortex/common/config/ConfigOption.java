package com.vortex.common.config;

import com.google.common.base.Predicate;

/*
* Class that validates and define the configurations options
* */

public class ConfigOption <T> extends TypedOption<T, T> {

    public ConfigOption(String name, String desc, T value) {
        this(name, desc, null, value);
    }

    @SuppressWarnings("unchecked")
    public ConfigOption(String name, String desc, Predicate<T> pred, T value) {
        this(name, false, desc, pred, (Class<T>) value.getClass(), value);
    }

    public ConfigOption(String name, boolean required, String desc, Predicate<T> pred, Class<T> type, T value) {
        super(name, required, desc, pred, type, value);
    }
}
