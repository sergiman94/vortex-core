
package com.vortex.common.config;

import com.google.common.base.Predicate;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public final class OptionChecker {

    public static <O> Predicate<O> disallowEmpty() {
        return new Predicate<O>() {
            @Override
            public boolean apply(@Nullable O o) {
                if (o == null) {
                    return false;
                }
                if (o instanceof String) {
                    return StringUtils.isNotBlank((String) o);
                }
                if (o.getClass().isArray() && (Array.getLength(o) == 0)) {
                    return false;
                }
                if (o instanceof Iterable &&
                    !((Iterable<?>) o).iterator().hasNext()) {
                    return false;
                }
                return true;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<O> allowValues(O... values) {
        return new Predicate<O>() {
            @Override
            public boolean apply(@Nullable O o) {
                return o != null && Arrays.asList(values).contains(o);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <O> Predicate<List<O>> inValues(O... values) {
        return new Predicate<List<O>>() {
            @Override
            public boolean apply(@Nullable List<O> o) {
                return o != null && Arrays.asList(values).containsAll(o);
            }
        };
    }

    public static <N extends Number> Predicate<N> positiveInt() {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                return number != null && number.longValue() > 0;
            }
        };
    }

    public static <N extends Number> Predicate<N> nonNegativeInt() {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                return number != null && number.longValue() >= 0;
            }
        };
    }

    public static <N extends Number> Predicate<N> rangeInt(N min, N max) {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                if (number == null) {
                    return false;
                }
                long value = number.longValue();
                return value >= min.longValue() && value <= max.longValue();
            }
        };
    }

    public static <N extends Number> Predicate<N> rangeDouble(N min, N max) {
        return new Predicate<N>() {
            @Override
            public boolean apply(@Nullable N number) {
                if (number == null) {
                    return false;
                }
                double value = number.doubleValue();
                return value >= min.doubleValue() && value <= max.doubleValue();
            }
        };
    }
}
