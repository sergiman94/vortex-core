package com.vortex.common.config;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.apache.commons.configuration.PropertyConverter;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/*
* This class checks if the data Type option (Boolean, List, etc) is valid and its value too
* */

public class TypedOption <T, R> {

    private static final Logger LOG = Log.logger(TypedOption.class);

    private static final Set<Class<?>> ACCEPTED_DATA_TYPES;
    private static final String ACCEPTED_DATA_TYPES_STRING;

    static {
        ACCEPTED_DATA_TYPES = ImmutableSet.of(
                Boolean.class,
                Short.class,
                Integer.class,
                Byte.class,
                Long.class,
                Float.class,
                Double.class,
                String.class,
                String[].class,
                Class.class,
                List.class
        );

        ACCEPTED_DATA_TYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATA_TYPES);
    }

    private final String name;
    private final String desc;
    private final boolean required;
    private final Class<T> dataType;
    private final T defaultValue;
    private final Predicate<T> checkFunc;

    // TODO: Research about Predicates in java
    @SuppressWarnings("unchecked")
    public TypedOption (String name, boolean required, String desc,
                        Predicate<T> pred, Class<T> type, T value) {

        E.checkNotNull(name, "name");
        E.checkNotNull(type, "dataType");

        this.name = name;
        this.dataType = (Class<T>) this.checkAndAssignDataType(type);
        this.defaultValue = value;
        this.required = required;
        this.desc = desc;
        this.checkFunc = pred;

        this.check(this.defaultValue);
    }

    // check and ASSIGN to config
    private Class<?> checkAndAssignDataType(Class<T> dataType){
        for (Class<?> clazz : ACCEPTED_DATA_TYPES) {
            // TODO: research about isAssignableFrom(dataType) method
            if(clazz.isAssignableFrom(dataType))
                return clazz;
        }

        String msg = String.format("Input data tyoe '%s' doesn't belong " + "to acceptable type set: [%s]", dataType, ACCEPTED_DATA_TYPES_STRING);
        throw new IllegalArgumentException(msg);
    }

    public String name() {return this.name;}

    public String desc() {return this.desc;}

    public boolean required() {return this.required;}

    public Class<T> dataType() {return this.dataType;}

    public R defaultValue() {return this.convert(defaultValue);}

    public R parsedConvert(String value) {
        T parsed = this.parse(value);
        this.check(parsed);
        return this.convert(parsed);
    }

    @SuppressWarnings("unchecked")
    protected T parse(String value) {return (T) this.parse(value, this.dataType);}

    // dynamic parser
    protected Object parse (String value, Class<?> dataType) {

        // return the value if is an string class
        if (dataType.equals(String.class)) {
            return value;

            // if is a class, try to retur the name and parse with that class
        } else if (dataType.equals(Class.class)) {
            try {
                if (value.startsWith("class")) {
                    value = value.substring("class".length()).trim();
                }

                return Class.forName(value);
            } catch (ClassNotFoundException e) {
                throw new ConfigException("Failed to parse Class from string '%s'", e, value);
            }
        } else if (List.class.isAssignableFrom(dataType)) {
            E.checkState(this.forList(), "List option can't be registered with class %s",
                    this.getClass().getSimpleName());
        }

        // Use PropertyConverter method `toXXX` convert value
        String methodTo = "to" + dataType.getSimpleName();

        try {
            Method method = PropertyConverter.class.getMethod(methodTo, Object.class);
            return method.invoke(null, value);
        } catch (ReflectiveOperationException e) {
            LOG.error("Invalid type of value '{}' for option '{}'",
                    value, this.name, e);
            throw new ConfigException(
                    "Invalid type of value '%s' for option '%s', " +
                            "expect '%s' type",
                    value, this.name, dataType.getSimpleName());
        }
    }

    // TODO: research about protected function in java
    @SuppressWarnings("unchecked")
    protected R convert (T value) {return (R) value;}

    // Check if datatype value is correct
    protected void check(Object value) {
        E.checkNotNull(value, "value", this.name);

        if (!this.dataType.isInstance(value)) {
            throw new ConfigException(
                    "Invalid type of value '%s' for option '%s', " +
                    "expect type %s but got %s", value, this.name,
                    this.dataType.getSimpleName(),
                    value.getClass().getSimpleName()
            );
        }

        // TODO: understand predicates to know what is going on here
        if (this.checkFunc != null) {
            @SuppressWarnings("unchecked")
            T result = (T) value;
            if (!this.checkFunc.apply(result)) {
                throw new ConfigException("invalid option value for '%s': %s", this.name, value);
            }
        }
    }

    protected  boolean forList() { return false;}

    @Override
    public String toString() {
        return String.format("[%s]%s=%s", this.dataType.getSimpleName(),
                this.name, this.defaultValue);
    }
}
