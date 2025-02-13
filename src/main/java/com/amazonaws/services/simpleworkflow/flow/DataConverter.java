/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow;

/**
 * Used by the framework to serialize/deserialize method parameters that need to
 * be sent over the wire. 
 * 
 * @author fateev
 */
public abstract class DataConverter {

    /**
     * Implements conversion of the single value.
     * 
     * @param value
     *            Java value to convert to String.
     * @return converted value
     * @throws DataConverterException
     *             if conversion of the value passed as parameter failed for any
     *             reason.
     */
    public abstract String toData(Object value) throws DataConverterException;

    /**
     * Implements conversion of the single value.
     * @param valueType - the Class object of the type to convert to
     * @param <T> - the type to convert the data to
     * @param content - Simple Workflow Data value to convert to a Java object.
     * @return T - converted Java object
     * @throws DataConverterException
     *             if conversion of the data passed as parameter failed for any
     *             reason.
     */
    public abstract <T> T fromData(String content, Class<T> valueType) throws DataConverterException;

}
