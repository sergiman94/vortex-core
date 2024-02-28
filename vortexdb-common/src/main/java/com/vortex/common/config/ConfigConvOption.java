
package com.vortex.common.config;

import com.google.common.base.Predicate;
import com.vortex.common.util.E;

import java.util.function.Function;

public class ConfigConvOption<T, R> extends TypedOption<T, R> {

    private final Function<T, R> converter;

    public ConfigConvOption(String name, String desc, Predicate<T> pred,
                            Function<T, R> convert, T value) {
        this(name, false, desc, pred, convert, value);
    }

    @SuppressWarnings("unchecked")
    public ConfigConvOption(String name, boolean required, String desc,
                            Predicate<T> pred, Function<T, R> convert,
                            T value) {
        super(name, required, desc, pred, (Class<T>) value.getClass(), value);
        E.checkNotNull(convert, "convert");
        this.converter = convert;
    }

    @Override
    public R convert(T value) {
        return this.converter.apply(value);
    }
}
