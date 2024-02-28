
package com.vortex.common.config;

import com.google.common.base.Predicate;
import com.vortex.common.util.E;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ConfigListConvOption<T, R> extends TypedOption<List<T>, List<R>> {

    private final Class<T> elemClass;
    private final Function<T, R> converter;

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                T... values) {
        this(name, false, desc, pred, convert, null, Arrays.asList(values));
    }

    @SuppressWarnings("unchecked")
    public ConfigListConvOption(String name, boolean required, String desc,
                                Predicate<List<T>> pred, Function<T, R> convert,
                                Class<T> clazz, List<T> values) {
        super(name, required, desc, pred,
              (Class<List<T>>) values.getClass(), values);
        E.checkNotNull(convert, "convert");
        if (clazz == null && values.size() > 0) {
            clazz = (Class<T>) values.get(0).getClass();
        }
        E.checkArgumentNotNull(clazz, "Element class can't be null");
        this.elemClass = clazz;
        this.converter = convert;
    }

    @Override
    protected boolean forList() {
        return true;
    }

    @Override
    protected List<T> parse(String value) {
        return ConfigListOption.convert(value, part -> {
            return this.parse(part, this.elemClass);
        });
    }

    @Override
    public List<R> convert(List<T> values) {
        List<R> results = new ArrayList<>(values.size());
        for (T value : values) {
            results.add(this.converter.apply(value));
        }
        return results;
    }
}
