package com.vortex.common.config;

import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class OptionSpace {
    private static final Logger LOG = Log.logger(OptionSpace.class);

    private static final Map<String, Class<? extends OptionHolder>> HOLDERS;
    private static final Map<String, TypedOption<?,?>> OPTIONS;
    private static final String INSTANCE_METHOD ="instance";

    // TODO: research about what is a concurrentHashMap
    static {
        HOLDERS = new ConcurrentHashMap<>();
        OPTIONS = new ConcurrentHashMap<>();
    }

    // this one registers the backends - server - rpc - auth
    public static void register(String module, String holder) {

        // check if the holder String parameter is actually a real holder
        ClassLoader classLoader = OptionSpace.class.getClassLoader();
        Class<?> clazz;

        try {
            clazz = classLoader.loadClass(holder);
        } catch (ClassNotFoundException e){
            throw new ConfigException("Failed to load class of option holder '%s'", e, holder);
        }

        // check subclass
        if (!OptionHolder.class.isAssignableFrom(clazz)) {
            throw new ConfigException("Class '%s' is not a subclass of OptionHolder", holder);
        }

        OptionHolder instance = null;
        Exception exception = null;

        try {
            // TODO: read about method Java
            Method method =  clazz.getMethod(INSTANCE_METHOD);
            if (!Modifier.isStatic(method.getModifiers()))
                throw new NoSuchMethodException(INSTANCE_METHOD);

            instance = (OptionHolder) method.invoke(null);

            if (instance == null) {
                exception = new ConfigException("Returned null from %s() method", INSTANCE_METHOD);
            }


        } catch (NoSuchMethodException e) {
            LOG.warn("Class {} does not has static method {}.",
                    holder, INSTANCE_METHOD);
            exception = e;
        } catch (InvocationTargetException e) {
            LOG.warn("Can't call static method {} from class {}.",
                    INSTANCE_METHOD, holder);
            exception = e;
        } catch (IllegalAccessException e) {
            LOG.warn("Illegal access while calling method {} from class {}.",
                    INSTANCE_METHOD, holder);
            exception = e;
        }

        if (exception != null) {
            throw new ConfigException("Failed to instantiate option holder: %s", exception, holder);
        }
    }

    // this one registers core - dist - tokens
    public static void register(String module, OptionHolder holder) {
        // Check exists

        if (HOLDERS.containsKey(module)) {
            LOG.warn("Already registered option holder: {} ({})",
                    module, HOLDERS.get(module));
        }

        E.checkArgumentNotNull(holder, "OptionHolder can't be null");

        HOLDERS.put(module, holder.getClass());
        OPTIONS.putAll((holder.options()));
        LOG.debug("Registered options for OptionHolder: {}",
                holder.getClass().getSimpleName());

    }

    public static Set<String> keys() {return Collections.unmodifiableSet(OPTIONS.keySet());}
    public static boolean containKey(String key) {return OPTIONS.containsKey(key);}
    public static TypedOption<?,?> get(String key) {return OPTIONS.get(key);}
}
