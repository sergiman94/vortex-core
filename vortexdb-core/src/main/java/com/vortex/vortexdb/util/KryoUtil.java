
package com.vortex.vortexdb.util;

import com.vortex.common.util.E;
import com.vortex.vortexdb.backend.BackendException;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

public final class KryoUtil {

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<>();

    public static Kryo kryo() {
        Kryo kryo = kryos.get();
        if (kryo != null) {
            return kryo;
        }

        kryo = new Kryo();
        registerSerializers(kryo);
        kryos.set(kryo);
        return kryo;
    }

    public static byte[] toKryo(Object value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos, 256)) {
            kryo().writeObject(output, value);
            output.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new BackendException("Failed to serialize: %s", e, value);
        }
    }

    public static <T> T fromKryo(byte[] value, Class<T> clazz) {
        E.checkState(value != null,
                     "Kryo value can't be null for '%s'",
                     clazz.getSimpleName());
        return kryo().readObject(new Input(value), clazz);
    }

    public static byte[] toKryoWithType(Object value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos, 256)) {
            kryo().writeClassAndObject(output, value);
            output.flush();
            return bos.toByteArray();
       } catch (IOException e) {
           throw new BackendException("Failed to serialize: %s", e, value);
       }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromKryoWithType(byte[] value) {
        E.checkState(value != null,  "Kryo value can't be null for object");
        return (T) kryo().readClassAndObject(new Input(value));
    }

    private static void registerSerializers(Kryo kryo) {
        kryo.addDefaultSerializer(UUID.class, new Serializer<UUID>() {

            @Override
            public UUID read(Kryo kryo, Input input, Class<UUID> c) {
                return new UUID(input.readLong(), input.readLong());
            }

            @Override
            public void write(Kryo kryo, Output output, UUID uuid) {
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            }
        });
    }
}
