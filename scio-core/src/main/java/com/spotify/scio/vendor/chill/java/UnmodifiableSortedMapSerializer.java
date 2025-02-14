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

import com.esotericsoftware.kryo.Serializer;
import com.spotify.scio.vendor.chill.IKryoRegistrar;
import com.spotify.scio.vendor.chill.SingleRegistrar;
import java.lang.reflect.Field;
import java.util.*;

/**
 * A kryo {@link Serializer} for unmodifiable sortable maps created via {@link
 * Collections#unmodifiableSortedMap(SortedMap)}.
 *
 * <p>Note: This serializer does not support cyclic references, so if one of the objects gets set
 * the list as attribute this might cause an error during deserialization.
 *
 * @author <a href="mailto:alex@chermenin.ru">Alex Chermenin</a>
 */
public class UnmodifiableSortedMapSerializer
    extends UnmodifiableJavaCollectionSerializer<SortedMap<?, ?>> {

  @SuppressWarnings("unchecked")
  public static IKryoRegistrar registrar() {
    return new SingleRegistrar(
        Collections.unmodifiableSortedMap(new TreeMap(Collections.EMPTY_MAP)).getClass(),
        new UnmodifiableSortedMapSerializer());
  }

  @Override
  protected Field getInnerField() throws Exception {
    return Class.forName("java.util.Collections$UnmodifiableSortedMap").getDeclaredField("sm");
  }

  @Override
  protected SortedMap<?, ?> newInstance(SortedMap<?, ?> m) {
    return Collections.unmodifiableSortedMap(m);
  }
}
