/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;

public class SortedBucketPrimaryAndSecondaryKeyedSource<K1, K2>
    extends SortedBucketSource<KV<K1, K2>> {
  private final Comparator<SortedBucketIO.ComparableKeyBytes> _comparator =
      new SortedBucketIO.PrimaryAndSecondaryKeyComparator();
  private final Class<K1> keyClassPrimary;
  private final Class<K2> keyClassSecondary;
  private Coder<K1> _keyCoderPrimary = null;
  private Coder<K2> _keyCoderSecondary = null;
  private Coder<KV<K1, K2>> _kvCoder = null;

  public SortedBucketPrimaryAndSecondaryKeyedSource(
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      List<BucketedInput<?>> sources,
      TargetParallelism targetParallelism,
      String metricsKey) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(keyClassPrimary, keyClassSecondary, sources, targetParallelism, 0, 1, metricsKey, null);
  }

  private SortedBucketPrimaryAndSecondaryKeyedSource(
      Class<K1> keyClassPrimary,
      Class<K2> keyClassSecondary,
      List<BucketedInput<?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism,
      String metricsKey,
      Long estimatedSizeBytes) {
    super(
        sources,
        targetParallelism,
        bucketOffsetId,
        effectiveParallelism,
        metricsKey,
        estimatedSizeBytes);
    this.keyClassPrimary = keyClassPrimary;
    this.keyClassSecondary = keyClassSecondary;
  }

  @Override
  public SortedBucketSource<KV<K1, K2>> createSplitSource(
      final int splitNum, final int totalParallelism, final long estSplitSize) {
    return new SortedBucketPrimaryAndSecondaryKeyedSource<>(
        keyClassPrimary,
        keyClassSecondary,
        sources,
        targetParallelism,
        bucketOffsetId + (splitNum * effectiveParallelism),
        totalParallelism,
        metricsKey,
        estSplitSize);
  }

  @Override
  protected Comparator<SortedBucketIO.ComparableKeyBytes> comparator() {
    return _comparator;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Coder<KV<K1, K2>> keyTypeCoder() {
    if (_kvCoder != null) return _kvCoder;
    Optional<Coder<K1>> c1 =
        sources.stream()
            .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
            .filter(sm -> sm.metadata.getKeyClass() == keyClassPrimary)
            .findFirst()
            .map(sm -> (Coder<K1>) sm.metadata.getKeyCoder());

    Optional<Coder<K2>> c2 =
        sources.stream()
            .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
            .filter(
                sm ->
                    sm.metadata.getKeyClassSecondary() != null
                        && sm.metadata.getKeyClassSecondary() == keyClassSecondary
                        && sm.metadata.getKeyCoderSecondary() != null)
            .findFirst()
            .map(sm -> (Coder<K2>) sm.metadata.getKeyCoderSecondary());

    if (!c1.isPresent())
      throw new NullPointerException("Could not infer coder for key class " + keyClassPrimary);
    if (!c2.isPresent())
      throw new NullPointerException("Could not infer coder for key class " + keyClassSecondary);

    _keyCoderPrimary = c1.get();
    _keyCoderSecondary = c2.get();
    _kvCoder = KvCoder.of(_keyCoderPrimary, _keyCoderSecondary);
    return _kvCoder;
  }

  @Override
  protected Function<SortedBucketIO.ComparableKeyBytes, KV<K1, K2>> toKeyFn() {
    keyTypeCoder(); // ensure coders defined
    return SortedBucketIO.ComparableKeyBytes.keyFnPrimaryAndSecondary(
        _keyCoderPrimary, _keyCoderSecondary);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyClassPrimary", keyClassPrimary.toString()));
    builder.add(DisplayData.item("keyClassSecondary", keyClassSecondary.toString()));
  }
}
