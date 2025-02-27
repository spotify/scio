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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class TestLists {

  private static final Kryo kryo = new Kryo();

  private static final Map<Class<?>, List<?>> map = new HashMap<Class<?>, List<?>>(9);

  static {
    ArraysAsListSerializer.registrar().apply(kryo);
    map.put(byte.class, Arrays.asList((byte) 1, (byte) 2, (byte) 3, (byte) 4));
    map.put(short.class, Arrays.asList((short) 1, (short) 2, (short) 3, (short) 4));
    map.put(int.class, Arrays.asList(1, 2, 3, 4));
    map.put(long.class, Arrays.asList(1L, 2L, 3L, 4L));
    map.put(char.class, Arrays.asList('1', '2', '3', '4'));
    map.put(float.class, Arrays.asList(1.0F, 2.0F, 3.0F, 4.0F));
    map.put(double.class, Arrays.asList(1.0, 2.0, 3.0, 4.0));
    map.put(boolean.class, Arrays.asList(true, false, true));
    map.put(String.class, Arrays.asList("one", "two", "three"));
    map.put(
        List.class,
        Arrays.asList(
            Arrays.asList(1, 2, 3, 4),
            Arrays.asList(1.0, 2.0, 3.0, 4.0),
            Arrays.asList("one", "two", "three")));
  }

  public static <T> T serializeAndDeserialize(T t) {
    Output output = new Output(1000, -1);
    kryo.writeClassAndObject(output, t);
    Input input = new Input(output.toBytes());
    return (T) kryo.readClassAndObject(input);
  }

  public static List<?> getList(Class<?> c) {
    return map.get(c);
  }
}
