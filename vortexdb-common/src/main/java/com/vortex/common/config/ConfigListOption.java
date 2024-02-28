package com.vortex.common.config;

import com.google.common.base.Predicate;
import com.vortex.common.util.E;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/*
* Class that checks and defines a list of configurations options
* */

public class ConfigListOption<T> extends ConfigOption<List<T>>{

    private final Class<T> elemClass;

    public ConfigListOption (String name, String desc, Predicate<List<T>> pred, T... values) {
        this(name, false, desc, pred, null, Arrays.asList(values));
    }

    @SuppressWarnings("unchecked")
    public ConfigListOption(String name, boolean required, String desc, Predicate<List<T>> pred, Class<T> clazz, List<T> values) {
        super(name, required, desc, pred, (Class<List<T>>) values.getClass(), values);

        if (clazz == null && values.size() > 0)
            clazz = (Class<T>) values.get(0).getClass();

        E.checkArgumentNotNull(clazz, "Element class can't be null");
        this.elemClass =  clazz;
    }

    @Override
    protected boolean forList() {
        return true;
    }

    @Override
    protected List<T> parse(String value) {
        return convert(value, part -> this.parse(part, this.elemClass));
    }

    // method that parse as a list
    @SuppressWarnings("unchecked")
    public static <T> List<T> convert(Object value, Function<String, ?> conv) {

        if (value instanceof List)
            return (List<T>) value;

        // if target data type is List, parse it as a list
        String str = (String) value;
        if (str.startsWith("[") && str.endsWith("]")) {
            str = str.substring(1, str.length() - 1);
        }

        String[] parts = str.split(",");
        List<T> results = new ArrayList<>(parts.length);
        for (String part : parts) {
            results.add((T) conv.apply(part.trim()));
        }
        return results;
    }
}
