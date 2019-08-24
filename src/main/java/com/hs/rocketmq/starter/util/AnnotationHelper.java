package com.hs.rocketmq.starter.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: HS
 * @Date: 2019/5/17 16:52
 */
public class AnnotationHelper {

    /**
     * 等到属性级别注解的信息
     * @param scannerClass：需要被扫描的class文件
     * @param allowInjectClass：注解的文件
     * @return
     */
    public static List<Annotation> getFieldAnnotation(Class<?> scannerClass , Class<? extends Annotation> allowInjectClass) {
        List<Annotation> annotations = new ArrayList<Annotation>();
        for(Field field: scannerClass.getDeclaredFields()) {
            if(!field.isAnnotationPresent(allowInjectClass)) {
                continue;
            }
            annotations.add(field.getAnnotation(allowInjectClass));
        }

        return annotations;
    }

    /**
     * 使用Java反射得到注解的信息
     * @param annotation
     * @param name
     * @return
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    public static Object getAnnotationInfo(Annotation annotation , String name) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if(annotation == null) {
            return null;
        }

        Method method = annotation.getClass().getDeclaredMethod(name, null);
        return method.invoke(annotation, null);
    }
    /**
     * 根据属性名获取属性值
     * @param fieldName 属性名
     * @Object o 实例化的类
     * */
    public static Object getFieldValueByName(String fieldName, Object o) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String firstLetter = fieldName.substring(0, 1).toUpperCase();
        String getter = "get" + firstLetter + fieldName.substring(1);
        Method method = o.getClass().getMethod(getter, new Class[] {});
        Object value = method.invoke(o, new Object[] {});
        return value;
    }
}
