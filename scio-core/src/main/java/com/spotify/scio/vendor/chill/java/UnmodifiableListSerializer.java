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
 * A kryo {@link Serializer} for unmodifiable lists created via {@link
 * Collections#unmodifiableList(List)}.
 *
 * <p>Note: This serializer does not support cyclic references, so if one of the objects gets set
 * the list as attribute this might cause an error during deserialization.
 *
 * @author <a href="mailto:alex@chermenin.ru">Alex Chermenin</a>
 */
public class UnmodifiableListSerializer extends UnmodifiableJavaCollectionSerializer<List<?>> {

  @SuppressWarnings("unchecked")
  public static IKryoRegistrar registrar() {
    return new IterableRegistrar(
        new SingleRegistrar(
            Collections.unmodifiableList(Collections.EMPTY_LIST).getClass(),
            new UnmodifiableListSerializer()),
        new SingleRegistrar(
            Collections.unmodifiableList(new LinkedList(Collections.EMPTY_LIST)).getClass(),
            new UnmodifiableListSerializer()));
  }

  @Override
  protected Field getInnerField() throws Exception {
    return Class.forName("java.util.Collections$UnmodifiableList").getDeclaredField("list");
  }

  @Override
  protected List<?> newInstance(List<?> l) {
    return Collections.unmodifiableList(l);
  }
}
