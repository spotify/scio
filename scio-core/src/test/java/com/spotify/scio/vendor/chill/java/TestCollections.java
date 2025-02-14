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
import java.util.*;

@SuppressWarnings("unchecked")
public class TestCollections {

  private static final Kryo kryo = new Kryo();

  private static final List<Integer> test_list = Arrays.asList(1, 2, 3, 4);

  private static final Set<Integer> test_set = new HashSet<Integer>(test_list);

  private static final Map<Integer, String> test_map = new HashMap<Integer, String>();

  static {
    PackageRegistrar.all().apply(kryo);

    test_map.put(1, "one");
    test_map.put(2, "two");
    test_map.put(3, "three");
    test_map.put(4, "four");
  }

  public static <T> T serializeAndDeserialize(T t) {
    Output output = new Output(1000, -1);
    kryo.writeClassAndObject(output, t);
    Input input = new Input(output.toBytes());
    return (T) kryo.readClassAndObject(input);
  }

  public static Collection<?> getUnmodifiableCollection() {
    return Collections.unmodifiableCollection(new ArrayList<Integer>(test_list));
  }

  public static List<?> getUnmodifiableArrayList() {
    return Collections.unmodifiableList(new ArrayList<Integer>(test_list));
  }

  public static List<?> getUnmodifiableLinkedList() {
    return Collections.unmodifiableList(new LinkedList<Integer>(test_list));
  }

  public static Map<?, ?> getUnmodifiableHashMap() {
    return Collections.unmodifiableMap(test_map);
  }

  public static Map<?, ?> getUnmodifiableTreeMap() {
    return Collections.unmodifiableSortedMap(new TreeMap<Integer, String>(test_map));
  }

  public static Set<?> getUnmodifiableHashSet() {
    return Collections.unmodifiableSet(test_set);
  }

  public static Set<?> getUnmodifiableTreeSet() {
    return Collections.unmodifiableSortedSet(new TreeSet<Integer>(test_set));
  }

  public static List<?> getEmptyList() {
    return Collections.emptyList();
  }

  public static Map<?, ?> getEmptyMap() {
    return Collections.emptyMap();
  }

  public static Set<?> getEmptySet() {
    return Collections.emptySet();
  }
}
