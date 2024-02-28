package com.vortex.vortexdb.type.define;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.vortex.common.util.E;
import com.vortex.common.util.CollectionUtil;
import com.vortex.vortexdb.backend.BackendException;

public interface SerialEnum {

    public byte code();

    // TODO: Research about what is this
    static Table<Class<?>, Byte, SerialEnum> table = HashBasedTable.create();

    public static void register (Class<? extends SerialEnum> clazz) {
        Object enums;
        try {
            enums = clazz.getMethod("values").invoke(null);
        }catch (Exception e) {
            throw new BackendException(e);
        }

        for (SerialEnum e: CollectionUtil.<SerialEnum>toList(enums)) {
            table.put(clazz, e.code(), e);
        }
    }

    public static <T extends SerialEnum> T fromCode(Class<T> clazz, byte code) {
        @SuppressWarnings("unchecked")
        T value = (T) table.get(clazz, code);
        if(value == null)
            E.checkArgument(false, "Can't construct %s from code %s", clazz.getSimpleName(), code);
        return value;
    }

    public static void registerInternalEnums() {
        SerialEnum.register(Action.class);
        SerialEnum.register(AggregateType.class);
        SerialEnum.register(Cardinality.class);
    }



}
