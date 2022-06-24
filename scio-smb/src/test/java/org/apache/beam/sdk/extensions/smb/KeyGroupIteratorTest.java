/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.ComparableKeyBytes;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link KeyGroupIterator}. */
public class KeyGroupIteratorTest {
  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final Comparator<ComparableKeyBytes> keyComparatorPrimary =
      new SortedBucketIO.PrimaryKeyComparator();
  private static final Comparator<ComparableKeyBytes> keyComparatorSecondary =
      new SortedBucketIO.PrimaryAndSecondaryKeyComparator();
  private static final Function<ComparableKeyBytes, String> keyFnPrimary =
      ComparableKeyBytes.keyFnPrimary(stringCoder);
  private static final Function<ComparableKeyBytes, KV<String, String>> keyFnSecondary =
      ComparableKeyBytes.keyFnPrimaryAndSecondary(stringCoder, stringCoder);
  private static final Function<String, ComparableKeyBytes> extractFnPrimary =
      s -> new ComparableKeyBytes(stringBytes(s.substring(0, 1)));
  private static final Function<String, ComparableKeyBytes> extractFnSecondary =
      s -> new ComparableKeyBytes(stringBytes(s.substring(0, 1)), stringBytes(s.substring(1, 2)));

  private static byte[] stringBytes(String s) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      stringCoder.encode(s, baos);
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SafeVarargs
  final List<Iterator<KV<ComparableKeyBytes, String>>> input(
      Function<String, ComparableKeyBytes> keyFn, Iterator<String>... iters) {
    return Arrays.stream(iters)
        .map(iter -> Iterators.transform(iter, s -> KV.of(keyFn.apply(s), s)))
        .collect(Collectors.toList());
  }

  @Test
  public void testEmptyIterator() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            Collections.singletonList(Collections.emptyIterator()), keyComparatorPrimary);
    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testEmptyIteratorSecondary() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            Collections.singletonList(Collections.emptyIterator()), keyComparatorSecondary);
    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeySingleValue() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnPrimary, Iterators.forArray("a1")), keyComparatorPrimary);

    Assert.assertTrue(iterator.hasNext());
    KV<ComparableKeyBytes, Iterator<String>> kv = iterator.next();
    String k = keyFnPrimary.apply(kv.getKey());
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeySingleValueSecondary() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnSecondary, Iterators.forArray("a1")), keyComparatorSecondary);

    Assert.assertTrue(iterator.hasNext());
    KV<ComparableKeyBytes, Iterator<String>> kv = iterator.next();
    KV<String, String> k = keyFnSecondary.apply(kv.getKey());
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals(KV.of("a", "1"), k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeyMultiValue() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnPrimary, Iterators.forArray("a1", "a2")), keyComparatorPrimary);

    Assert.assertTrue(iterator.hasNext());
    KV<ComparableKeyBytes, Iterator<String>> kv = iterator.next();
    String k = keyFnPrimary.apply(kv.getKey());
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a2", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeyMultiValueSecondary() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnSecondary, Iterators.forArray("a1a", "a1b")), keyComparatorSecondary);

    Assert.assertTrue(iterator.hasNext());
    KV<ComparableKeyBytes, Iterator<String>> kv = iterator.next();
    KV<String, String> k = keyFnSecondary.apply(kv.getKey());
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals(KV.of("a", "1"), k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1a", vi.next());
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1b", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testMultiKeySingleValue() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnPrimary, Iterators.forArray("a1", "b1")), keyComparatorPrimary);

    KV<ComparableKeyBytes, Iterator<String>> kv;
    String k;
    Iterator<String> vi;

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = keyFnPrimary.apply(kv.getKey());
    vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = keyFnPrimary.apply(kv.getKey());
    vi = kv.getValue();
    Assert.assertEquals("b", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("b1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testMultiKeySingleValueSecondary() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnSecondary, Iterators.forArray("a1", "b1")), keyComparatorSecondary);

    KV<ComparableKeyBytes, Iterator<String>> kv;
    KV<String, String> k;
    Iterator<String> vi;

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = keyFnSecondary.apply(kv.getKey());
    vi = kv.getValue();
    Assert.assertEquals(KV.of("a", "1"), k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = keyFnSecondary.apply(kv.getKey());
    vi = kv.getValue();
    Assert.assertEquals(KV.of("b", "1"), k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("b1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testMultiKeyMultiElement() {
    testIteratorPrimary(
        Lists.newArrayList("a1", "a2", "b1", "b2"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1", "a2")),
            KV.of("b", Lists.newArrayList("b1", "b2"))));
    testIteratorPrimary(
        Lists.newArrayList("a1", "b1", "b2"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1")), KV.of("b", Lists.newArrayList("b1", "b2"))));
    testIteratorPrimary(
        Lists.newArrayList("a1", "a2", "b1"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1", "a2")), KV.of("b", Lists.newArrayList("b1"))));
    testIteratorPrimary(
        Lists.newArrayList("a1", "b1", "b2", "c1", "c2", "c3"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1")),
            KV.of("b", Lists.newArrayList("b1", "b2")),
            KV.of("c", Lists.newArrayList("c1", "c2", "c3"))));

    testIteratorSecondary(
        Lists.newArrayList("ab1", "ab2", "bc1", "bc2"),
        Lists.newArrayList(
            KV.of(KV.of("a", "b"), Lists.newArrayList("ab1", "ab2")),
            KV.of(KV.of("b", "c"), Lists.newArrayList("bc1", "bc2"))));
    testIteratorSecondary(
        Lists.newArrayList("ab1", "bc1", "bc2"),
        Lists.newArrayList(
            KV.of(KV.of("a", "b"), Lists.newArrayList("ab1")),
            KV.of(KV.of("b", "c"), Lists.newArrayList("bc1", "bc2"))));
    testIteratorSecondary(
        Lists.newArrayList("ab1", "ab2", "bc1"),
        Lists.newArrayList(
            KV.of(KV.of("a", "b"), Lists.newArrayList("ab1", "ab2")),
            KV.of(KV.of("b", "c"), Lists.newArrayList("bc1"))));
    testIteratorSecondary(
        Lists.newArrayList("ab1", "bc1", "bc2", "cd1", "cd2", "cd3"),
        Lists.newArrayList(
            KV.of(KV.of("a", "b"), Lists.newArrayList("ab1")),
            KV.of(KV.of("b", "c"), Lists.newArrayList("bc1", "bc2")),
            KV.of(KV.of("c", "d"), Lists.newArrayList("cd1", "cd2", "cd3"))));
  }

  @Test
  public void testMultiIterators() {
    testMultiIteratorPrimary(
        ImmutableList.of(
            Lists.newArrayList("a1", "b1", "b3", "c2", "c4"),
            Lists.newArrayList("b2", "b4", "c1", "c3", "d1")),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1")),
            KV.of("b", Lists.newArrayList("b1", "b2", "b3", "b4")),
            KV.of("c", Lists.newArrayList("c1", "c2", "c3", "c4")),
            KV.of("d", Lists.newArrayList("d1"))));

    testMultiIteratorSecondary(
        ImmutableList.of(
            Lists.newArrayList("a1", "b1a", "b1d", "c1b", "c1d"),
            Lists.newArrayList("b1b", "b1c", "b1e", "c1a", "c1c", "d1")),
        Lists.newArrayList(
            KV.of(KV.of("a", "1"), Lists.newArrayList("a1")),
            KV.of(KV.of("b", "1"), Lists.newArrayList("b1a", "b1b", "b1c", "b1d", "b1e")),
            KV.of(KV.of("c", "1"), Lists.newArrayList("c1a", "c1b", "c1c", "c1d")),
            KV.of(KV.of("d", "1"), Lists.newArrayList("d1"))));
  }

  private void testIteratorPrimary(List<String> data, List<KV<String, List<String>>> expected) {
    testMultiIteratorPrimary(Collections.singletonList(data), expected);
  }

  private void testMultiIteratorPrimary(
      List<List<String>> data, List<KV<String, List<String>>> expected) {
    testIterator(data, expected, extractFnPrimary, keyComparatorPrimary, keyFnPrimary);
  }

  private void testIteratorSecondary(
      List<String> data, List<KV<KV<String, String>, List<String>>> expected) {
    testMultiIteratorSecondary(Collections.singletonList(data), expected);
  }

  private void testMultiIteratorSecondary(
      List<List<String>> data, List<KV<KV<String, String>, List<String>>> expected) {
    testIterator(data, expected, extractFnSecondary, keyComparatorSecondary, keyFnSecondary);
  }

  private <KeyType> void testIterator(
      List<List<String>> data,
      List<KV<KeyType, List<String>>> expected,
      Function<String, ComparableKeyBytes> extractFn,
      Comparator<ComparableKeyBytes> comparator,
      Function<ComparableKeyBytes, KeyType> keyFn) {
    @SuppressWarnings("unchecked")
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<String>(
            input(extractFn, data.stream().map(List::iterator).toArray(Iterator[]::new)),
            comparator);
    List<KV<KeyType, List<String>>> actual = new ArrayList<>();
    iterator.forEachRemaining(
        kv ->
            actual.add(
                KV.of(
                    keyFn.apply(kv.getKey()),
                    Lists.newArrayList(kv.getValue()).stream()
                        .sorted()
                        .collect(Collectors.toList()))));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testIllegalStates() {
    KeyGroupIterator<String> iterator =
        new KeyGroupIterator<>(
            input(extractFnPrimary, Iterators.forArray("a1", "a2", "b1")), keyComparatorPrimary);
    Assert.assertTrue(iterator.hasNext());
    KV<ComparableKeyBytes, Iterator<String>> kv = iterator.next();
    String k = keyFnPrimary.apply(kv.getKey());
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertTrue(vi.hasNext());
    Assert.assertThrows(
        "Previous Iterator<ValueT> not fully iterated",
        IllegalStateException.class,
        iterator::hasNext);
    Assert.assertThrows(
        "Previous Iterator<ValueT> not fully iterated",
        IllegalStateException.class,
        iterator::next);
  }
}
