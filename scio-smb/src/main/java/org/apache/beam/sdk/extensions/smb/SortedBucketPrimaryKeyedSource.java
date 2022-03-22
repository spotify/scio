package org.apache.beam.sdk.extensions.smb;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;

public class SortedBucketPrimaryKeyedSource<K> extends SortedBucketSource<K> {
  private final Class<K> keyClassPrimary;
  private Coder<K> _keyCoderPrimary = null;
  private final Comparator<SortedBucketIO.ComparableKeyBytes> _comparator =
      new SortedBucketIO.PrimaryKeyComparator();

  public SortedBucketPrimaryKeyedSource(
      Class<K> keyClassPrimary,
      List<BucketedInput<?>> sources,
      TargetParallelism targetParallelism,
      String metricsKey) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(keyClassPrimary, sources, targetParallelism, 0, 1, metricsKey, null);
  }

  private SortedBucketPrimaryKeyedSource(
      Class<K> keyClassPrimary,
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
  }

  @Override
  public SortedBucketSource<K> createSplitSource(
      final int splitNum, final int totalParallelism, final long estSplitSize) {
    return new SortedBucketPrimaryKeyedSource<>(
        keyClassPrimary,
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
  public Coder<K> keyTypeCoder() {
    if (_keyCoderPrimary != null) return _keyCoderPrimary;
    Optional<Coder<K>> c =
        sources.stream()
            .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
            .filter(sm -> sm.metadata.getKeyClass() == keyClassPrimary)
            .findFirst()
            .map(sm -> (Coder<K>) sm.metadata.getKeyCoder());
    if (!c.isPresent())
      throw new NullPointerException("Could not infer coder for key class " + keyClassPrimary);
    _keyCoderPrimary = c.get();
    return _keyCoderPrimary;
  }

  @Override
  protected Function<SortedBucketIO.ComparableKeyBytes, K> toKeyFn() {
    return SortedBucketIO.ComparableKeyBytes.keyFnPrimary(keyTypeCoder());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyClassPrimary", keyClassPrimary.toString()));
  }
}
