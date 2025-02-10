/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.common;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is for internal use only and may be changed or removed without
 * prior notice.
 * 
 */
public final class FlowHelpers {

    public enum PredefinedDuration {
        NONE
    }

    private static final Map<Class<?>, Object> defaultValues = new ConcurrentHashMap<Class<?>, Object>();

    static {
        defaultValues.put(byte.class, Byte.valueOf((byte) 0));
        defaultValues.put(char.class, Character.valueOf((char) 0));
        defaultValues.put(short.class, Short.valueOf((short) 0));
        defaultValues.put(int.class, Integer.valueOf(0));
        defaultValues.put(long.class, Long.valueOf(0));
        defaultValues.put(float.class, Float.valueOf(0));
        defaultValues.put(double.class, Double.valueOf(0));
        defaultValues.put(boolean.class, Boolean.FALSE);
    }

    public static String secondsToDuration(Long seconds) {
        if (seconds == null || seconds == FlowConstants.NONE) {
            return PredefinedDuration.NONE.toString();
        }
        else if (seconds == FlowConstants.USE_REGISTERED_DEFAULTS) {
            return null;
        }

        return Long.toString(seconds);
    }

    public static long durationToSeconds(String duration) {
        if (duration == null || duration.equals(PredefinedDuration.NONE.toString())) {
            return FlowConstants.NONE;
        }
        else {
            return Long.parseLong(duration);
        }
    }

    public static Object[] validateInput(Method method, Object[] args) {
        Class<?>[] paramterTypes = method.getParameterTypes();
        int numberOfParameters = paramterTypes.length;
        if (args == null || args.length != numberOfParameters) {
            throw new IllegalStateException("Number of parameters does not match args size.");
        }

        int index = 0;
        for (Class<?> paramType : paramterTypes) {
            Object argument = args[index];
            if (argument != null && !paramType.isAssignableFrom(argument.getClass())) {
                throw new IllegalStateException("Param type '" + paramType.getName() + "' is not assigable from '"
                        + argument.getClass().getName() + "'.");
            }

            index++;
        }

        return args;
    }

    public static String taskPriorityToString(Integer taskPriority) {
        if (taskPriority == null) {
            return null;
        }
        return String.valueOf(taskPriority);
    }

    public static int taskPriorityToInt(String taskPriority) {
        if (taskPriority == null) {
            return FlowConstants.DEFAULT_TASK_PRIORITY;
        }
        else {
            return Integer.parseInt(taskPriority);
        }
    }
    
    /**
     * Returns array of parameter values which is the same as values if types
     * and values parameter have the same size. If values is shorter than types
     * then missing elements of returned array are filled with default values.
     * <p>
     * Used to support backward compatible changes in activities and workflow
     * APIs.
     */
    public static Object[] getInputParameters(Class<?>[] types, Object[] values) {

        int valuesLength = 0;
        if (values != null) {
            valuesLength = values.length;
        }
        if (valuesLength == types.length) {
            return values;
        }
        Object[] result;
        if (values == null) {
            result = new Object[types.length];
        }
        else {
            result = Arrays.copyOf(values, types.length);
        }
        for (int i = valuesLength; i < types.length; i++) {
            result[i] = getDefaultValue(types[i]);
        }
        return result;
    }

    public static Object getDefaultValue(Class<?> clazz) {
        return defaultValues.get(clazz);
    }

}
