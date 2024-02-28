package com.vortex.client.structure.schema;

import com.vortex.client.exception.InvalidOperationException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class BuilderProxy<T> implements InvocationHandler {

    private boolean finished;
    private T builder;
    private T proxy;

    @SuppressWarnings("unchecked")
    public BuilderProxy(T builder) {
        this.finished = false;
        this.builder = builder;
        this.proxy = (T) Proxy.newProxyInstance(
                     builder.getClass().getClassLoader(),
                     builder.getClass().getInterfaces(),
                     this);
    }

    public T proxy() {
        return this.proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
                         throws Throwable {
        if (this.finished) {
            throw new InvalidOperationException(
                      "Can't access builder which is completed");
        }
        // The result may be equal this.builder, like method `asText`
        Object result = method.invoke(this.builder, args);
        if (result == this.builder) {
            result = this.proxy;
        } else {
            this.finished = true;
        }
        return result;
    }
}
