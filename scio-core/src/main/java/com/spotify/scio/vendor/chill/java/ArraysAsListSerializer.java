/*
 * Copyright 2010 Martin Grotzke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.spotify.scio.vendor.chill.java;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.spotify.scio.vendor.chill.IKryoRegistrar;
import com.spotify.scio.vendor.chill.SingleRegistrar;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A kryo {@link Serializer} for lists created via {@link Arrays#asList(Object...)}.
 *
 * <p>Note: This serializer does not support cyclic references, so if one of the objects gets set
 * the list as attribute this might cause an error during deserialization.
 *
 * @author <a href="mailto:martin.grotzke@javakaffee.de">Martin Grotzke</a>
 */
public class ArraysAsListSerializer extends Serializer<List<?>> {

  private static final Map<Class<?>, Class<?>> primitives =
      new HashMap<Class<?>, Class<?>>(8, 1.0F);

  static {
    primitives.put(byte.class, Byte.class);
    primitives.put(short.class, Short.class);
    primitives.put(int.class, Integer.class);
    primitives.put(long.class, Long.class);
    primitives.put(char.class, Character.class);
    primitives.put(float.class, Float.class);
    primitives.put(double.class, Double.class);
    primitives.put(boolean.class, Boolean.class);
  }

  public static IKryoRegistrar registrar() {
    return new SingleRegistrar(Arrays.asList("").getClass(), new ArraysAsListSerializer());
  }

  private Field _arrayField;

  public ArraysAsListSerializer() {
    try {
      _arrayField = Class.forName("java.util.Arrays$ArrayList").getDeclaredField("a");
      _arrayField.setAccessible(true);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<?> read(final Kryo kryo, final Input input, final Class<List<?>> type) {
    final int length = input.readInt(true);
    Class<?> componentType = kryo.readClass(input).getType();
    try {
      final Object items = Array.newInstance(getBoxedClass(componentType), length);
      for (int i = 0; i < length; i++) {
        Array.set(items, i, kryo.readClassAndObject(input));
      }
      return Arrays.asList((Object[]) items);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(final Kryo kryo, final Output output, final List<?> obj) {
    try {
      final Object[] array = (Object[]) _arrayField.get(obj);
      output.writeInt(array.length, true);
      final Class<?> componentType = array.getClass().getComponentType();
      kryo.writeClass(output, componentType);
      for (final Object item : array) {
        kryo.writeClassAndObject(output, item);
      }
    } catch (final RuntimeException e) {
      // Don't eat and wrap RuntimeExceptions because the ObjectBuffer.write...
      // handles SerializationException specifically (resizing the buffer)...
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Class<?> getBoxedClass(final Class<?> c) {
    if (c.isPrimitive()) {
      Class<?> x;
      return (x = primitives.get(c)) != null ? x : c;
    }
    return c;
  }
}
