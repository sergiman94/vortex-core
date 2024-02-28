
package com.vortex.common.util;

import com.vortex.common.iterator.ExtendableIterator;
import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class ReflectionUtil {

    public static boolean isSimpleType(Class<?> type) {
        if (type.isPrimitive() ||
            type.equals(String.class) ||
            type.equals(Boolean.class) ||
            type.equals(Character.class) ||
            NumericUtil.isNumber(type)) {
            return true;
        }
        return false;
    }

    public static List<Method> getMethodsAnnotatedWith(
                               Class<?> type,
                               Class<? extends Annotation> annotation,
                               boolean withSuperClass) {
        final List<Method> methods = new LinkedList<>();
        Class<?> klass = type;
        do {
            for (Method method : klass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != Object.class && withSuperClass);
        return methods;
    }

    public static List<CtMethod> getMethodsAnnotatedWith(
                                 CtClass type,
                                 Class<? extends Annotation> annotation,
                                 boolean withSuperClass)
                                 throws NotFoundException {
        final List<CtMethod> methods = new LinkedList<>();

        CtClass klass = type;
        do {
            for (CtMethod method : klass.getDeclaredMethods()) {
                if (method.hasAnnotation(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != null && withSuperClass);
        return methods;
    }

    public static Iterator<ClassInfo> classes(String... packages)
                                              throws IOException {
        ClassPath path = ClassPath.from(ReflectionUtil.class.getClassLoader());
        ExtendableIterator<ClassInfo> results = new ExtendableIterator<>();
        for (String p : packages) {
            results.extend(path.getTopLevelClassesRecursive(p).iterator());
        }
        return results;
    }

    public static List<String> superClasses(String clazz)
                                            throws NotFoundException {
        CtClass klass = ClassPool.getDefault().get(clazz);
        CtClass parent = klass.getSuperclass();

        List<String> results = new LinkedList<>();
        while (parent != null) {
            results.add(parent.getName());
            parent = parent.getSuperclass();
        }
        return Lists.reverse(results);
    }

    public static List<String> nestedClasses(String clazz)
                                             throws NotFoundException {
        CtClass klass = ClassPool.getDefault().get(clazz);

        List<String> results = new LinkedList<>();
        for (CtClass nested : klass.getNestedClasses()) {
            results.add(nested.getName());
        }
        return results;
    }

    public static String packageName(String clazz) {
        int offset = clazz.lastIndexOf(".");
        if (offset > 0) {
            return clazz.substring(0, offset);
        }
        return "";
    }
}
