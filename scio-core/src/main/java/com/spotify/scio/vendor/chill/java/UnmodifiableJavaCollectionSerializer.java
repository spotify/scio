/*
 * Copyright 2016 Alex Chermenin
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
import java.lang.reflect.Field;

/**
 * A kryo base {@link Serializer} for unmodifiable Java collections.
 *
 * <p>Note: This serializer does not support cyclic references, so if one of the objects gets set
 * the list as attribute this might cause an error during deserialization.
 *
 * @author <a href="mailto:alex@chermenin.ru">Alex Chermenin</a>
 */
abstract class UnmodifiableJavaCollectionSerializer<T> extends Serializer<T> {

  protected abstract T newInstance(T o);

  protected abstract Field getInnerField() throws Exception;

  private final Field innerField;

  UnmodifiableJavaCollectionSerializer() {
    try {
      innerField = getInnerField();
      innerField.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(Kryo kryo, Input input, Class<T> type) {
    try {
      T u = (T) kryo.readClassAndObject(input);
      return newInstance(u);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Kryo kryo, Output output, T object) {
    try {
      T u = (T) innerField.get(object);
      kryo.writeClassAndObject(output, u);
    } catch (RuntimeException e) {
      // Don't eat and wrap RuntimeExceptions because the ObjectBuffer.write...
      // handles SerializationException specifically (resizing the buffer)...
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
