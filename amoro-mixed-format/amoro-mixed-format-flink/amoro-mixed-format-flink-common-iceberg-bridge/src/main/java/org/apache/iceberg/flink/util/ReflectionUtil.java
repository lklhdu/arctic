/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.util;

import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;

/** An util for reflection. */
public class ReflectionUtil {

  private ReflectionUtil() {}

  /** get interfaces of class and its parent */
  public static Class<?>[] getAllInterface(Class<?> clazz) {
    if (clazz.equals(Object.class)) {
      return new Class[] {};
    }
    Class<?>[] current = clazz.getInterfaces();
    Class<?> superClass = clazz.getSuperclass();
    Class<?>[] superInterfaces = getAllInterface(superClass);

    Set<Class<?>> all = Sets.newHashSet();
    all.addAll(Arrays.asList(current));
    all.addAll(Arrays.asList(superInterfaces));

    Class<?>[] deDuplicated = new Class[all.size()];
    return all.toArray(deDuplicated);
  }

  public static <O, V> V getField(Class<O> clazz, O obj, String fieldName) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      Object fieldValue = field.get(obj);
      return fieldValue == null ? null : (V) fieldValue;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
