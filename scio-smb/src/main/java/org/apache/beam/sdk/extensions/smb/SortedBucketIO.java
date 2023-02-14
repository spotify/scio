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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.SortedBucketPreKeyedSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.NewBucketMetadataFn;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFnWithSideInputContext;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.LoggerFactory;

/**
 * Sorted-bucket files are {@code PCollection<V>}s written with {@link SortedBucketSink} that can be
 * efficiently merged without shuffling with {@link SortedBucketSource}. When writing, values are
 * grouped by key into buckets, sorted by key within a bucket, and written to files. When reading,
 * key-values in matching buckets are read in a merge-sort style, reducing shuffle.
 */
public class SortedBucketIO {

  static final int DEFAULT_NUM_SHARDS = 1;
  static final HashType DEFAULT_HASH_TYPE = HashType.MURMUR3_128;
  static final int DEFAULT_SORTER_MEMORY_MB = 1024;
  static final String DEFAULT_FILENAME_PREFIX = "bucket";
  static final TargetParallelism DEFAULT_PARALLELISM = TargetParallelism.auto();

  /** Co-groups sorted-bucket sources with the same primary sort key. */
  public static <K> CoGbkBuilder<K> read(Class<K> primaryKeyClass) {
    return new CoGbkBuilder<>(primaryKeyClass);
  }

  /** Co-groups sorted-bucket sources with the same primary and secondary sort key. */
  public static <K1, K2> CoGbkWithSecondaryBuilder<K1, K2> read(
      Class<K1> primaryKeyClass, Class<K2> secondaryKeyClass) {
    return new CoGbkWithSecondaryBuilder<>(primaryKeyClass, secondaryKeyClass);
  }

  private static void validateInputNameUniqueness(List<BucketedInput<?>> inputs) {
    HashSet<String> inputNames = new HashSet<>();
    inputs.stream()
        .forEach(i -> {
              if (!inputNames.add(i.getTupleTag().getId())) {
                throw new IllegalArgumentException(
                    String.format(
                        "Tuple tag name for each smb input source needs to be unique. Input "
                        + "source name \"%s\" is used at least twice.", i.getTupleTag().getId()
                    )
                );
              }
            }
        );
  }

  /**
   * Builder for sorted-bucket {@link CoGbk}.
   */
  public static class CoGbkBuilder<K1> {

    private final Class<K1> primaryKeyClass;

    private CoGbkBuilder(Class<K1> primaryKeyClass) {
      this.primaryKeyClass = primaryKeyClass;
    }

    /** Returns a new {@link CoGbk} with the given first sorted-bucket source in {@link Read}. */
    public CoGbk<K1> of(Read<?>... read) {
      List<BucketedInput<?>> readInputs =
          Arrays.stream(read).map(r -> r.toBucketedInput(SortedBucketSource.Keying.PRIMARY))
              .collect(Collectors.toList());
      validateInputNameUniqueness(readInputs);
      return new CoGbk<>(
          primaryKeyClass,
          readInputs,
          DEFAULT_PARALLELISM,
          null);
    }
  }

  /** Builder for sorted-bucket {@link CoGbk}. */
  public static class CoGbkWithSecondaryBuilder<K1, K2> {
    private final Class<K1> primaryKeyClass;
    private final Class<K2> secondaryKeyClass;

    private CoGbkWithSecondaryBuilder(Class<K1> primaryKeyClass, Class<K2> secondaryKeyClass) {
      this.primaryKeyClass = primaryKeyClass;
      this.secondaryKeyClass = secondaryKeyClass;
    }

    /**
     * Returns a new {@link CoGbkWithSecondary} with the given first sorted-bucket source in {@link
     * Read}.
     */
    public CoGbkWithSecondary<K1, K2> of(Read<?> read) {
      return new CoGbkWithSecondary<>(
          primaryKeyClass,
          secondaryKeyClass,
          Collections.singletonList(
              read.toBucketedInput(SortedBucketSource.Keying.PRIMARY_AND_SECONDARY)),
          DEFAULT_PARALLELISM,
          null);
    }
  }

  public interface Transformable<KeyType, K1, K2> {
    <V> AbsCoGbkTransform<KeyType, V> transform(TransformOutput<K1, K2, V> transform);
  }

  /**
   * A {@link PTransform} for co-grouping sorted-bucket sources using {@link SortedBucketSource}.
   */
  public static class CoGbk<K> extends PTransform<PBegin, PCollection<KV<K, CoGbkResult>>>
      implements Transformable<K, K, Void> {
    private final Class<K> keyClass;
    private final List<BucketedInput<?>> inputs;
    private final TargetParallelism targetParallelism;
    private final String metricsKey;

    private CoGbk(
        Class<K> keyClass,
        List<BucketedInput<?>> inputs,
        TargetParallelism targetParallelism,
        String metricsKey) {
      this.keyClass = keyClass;
      this.inputs = inputs;
      this.targetParallelism = targetParallelism;
      this.metricsKey = metricsKey;
    }

    /**
     * Returns a new {@link CoGbk} that is the same as this, appended with the given sorted-bucket
     * source in {@link Read}.
     */
    public CoGbk<K> and(Read<?> read) {
      ImmutableList<BucketedInput<?>> newReads =
          ImmutableList.<BucketedInput<?>>builder()
              .addAll(inputs)
              .add(read.toBucketedInput(SortedBucketSource.Keying.PRIMARY))
              .build();
      validateInputNameUniqueness(newReads);
      return new CoGbk<>(keyClass, newReads, targetParallelism, metricsKey);
    }

    public CoGbk<K> withTargetParallelism(TargetParallelism targetParallelism) {
      return new CoGbk<>(keyClass, inputs, targetParallelism, metricsKey);
    }

    public CoGbk<K> withMetricsKey(String metricsKey) {
      return new CoGbk<>(keyClass, inputs, targetParallelism, metricsKey);
    }

    public <V> CoGbkTransform<K, V> transform(TransformOutput<K, Void, V> transform) {
      return new CoGbkTransform<>(keyClass, inputs, targetParallelism, transform);
    }

    @Override
    public PCollection<KV<K, CoGbkResult>> expand(PBegin input) {
      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              new SortedBucketPrimaryKeyedSource<>(
                  keyClass, inputs, targetParallelism, metricsKey)));
    }
  }

  /**
   * A {@link PTransform} for co-grouping sorted-bucket sources using {@link
   * SortedBucketPrimaryAndSecondaryKeyedSource}.
   */
  public static class CoGbkWithSecondary<K1, K2>
      extends PTransform<PBegin, PCollection<KV<KV<K1, K2>, CoGbkResult>>>
      implements Transformable<KV<K1, K2>, K1, K2> {
    private final Class<K1> keyClassPrimary;
    private final Class<K2> keyClassSecondary;
    private final List<BucketedInput<?>> inputs;
    private final TargetParallelism targetParallelism;
    private final String metricsKey;

    private CoGbkWithSecondary(
        Class<K1> keyClassPrimary,
        Class<K2> keyClassSecondary,
        List<BucketedInput<?>> inputs,
        TargetParallelism targetParallelism,
        String metricsKey) {
      this.keyClassPrimary = keyClassPrimary;
      this.keyClassSecondary = keyClassSecondary;
      this.inputs = inputs;
      this.targetParallelism = targetParallelism;
      this.metricsKey = metricsKey;
    }

    /**
     * Returns a new {@link CoGbk} that is the same as this, appended with the given sorted-bucket
     * source in {@link Read}.
     */
    public CoGbkWithSecondary<K1, K2> and(Read<?> read) {
      ImmutableList<BucketedInput<?>> newReads =
          ImmutableList.<BucketedInput<?>>builder()
              .addAll(inputs)
              .add(read.toBucketedInput(SortedBucketSource.Keying.PRIMARY_AND_SECONDARY))
              .build();
      validateInputNameUniqueness(newReads);
      return new CoGbkWithSecondary<>(
          keyClassPrimary, keyClassSecondary, newReads, targetParallelism, metricsKey);
    }

    public CoGbkWithSecondary<K1, K2> withTargetParallelism(TargetParallelism targetParallelism) {
      return new CoGbkWithSecondary<>(
          keyClassPrimary, keyClassSecondary, inputs, targetParallelism, metricsKey);
    }

    public CoGbkWithSecondary<K1, K2> withMetricsKey(String metricsKey) {
      return new CoGbkWithSecondary<>(
          keyClassPrimary, keyClassSecondary, inputs, targetParallelism, metricsKey);
    }

    public <V> CoGbkTransformWithSecondary<K1, K2, V> transform(
        TransformOutput<K1, K2, V> transform) {
      return new CoGbkTransformWithSecondary<K1, K2, V>(
          keyClassPrimary, keyClassSecondary, inputs, targetParallelism, transform);
    }

    @Override
    public PCollection<KV<KV<K1, K2>, CoGbkResult>> expand(PBegin input) {
      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              new SortedBucketPrimaryAndSecondaryKeyedSource<K1, K2>(
                  keyClassPrimary, keyClassSecondary, inputs, targetParallelism, metricsKey)));
    }
  }

  public abstract static class AbsCoGbkTransform<KeyType, V>
      extends PTransform<PBegin, WriteResult> {
    protected TransformFn<KeyType, V> toFinalResultT;
    protected TransformFnWithSideInputContext<KeyType, V> toFinalResultTWithSides;
    protected Iterable<PCollectionView<?>> sides;
    protected final List<BucketedInput<?>> inputs;
    protected final TargetParallelism targetParallelism;

    private final ResourceId outputDirectory;
    private final ResourceId tempDirectory;
    private final NewBucketMetadataFn<?, ?, V> newBucketMetadataFn;
    private final FileOperations<V> fileOperations;
    private final String filenameSuffix;
    private final String filenamePrefix;

    public AbsCoGbkTransform(
        List<BucketedInput<?>> inputs,
        TargetParallelism targetParallelism,
        TransformOutput<?, ?, V> transform) {
      this.inputs = inputs;
      this.targetParallelism = targetParallelism;

      this.outputDirectory = transform.getOutputDirectory();
      this.tempDirectory = transform.getTempDirectory();
      this.newBucketMetadataFn = transform.getNewBucketMetadataFn();
      this.fileOperations = transform.getFileOperations();
      this.filenameSuffix = transform.getFilenameSuffix();
      this.filenamePrefix = transform.getFilenamePrefix();
    }

    protected abstract Function<SortedBucketIO.ComparableKeyBytes, KeyType> toKeyFn();

    protected abstract Comparator<SortedBucketIO.ComparableKeyBytes> comparator();

    protected void xformCheck() {
      Preconditions.checkState(
          !((toFinalResultT == null) && (toFinalResultTWithSides == null)), // at least one defined
          "One of TransformFn<K, V> or TransformFnWithSideInputContext<K, V> must be set by via()");
      Preconditions.checkState(
          !((toFinalResultT != null) && (toFinalResultTWithSides != null)), // only one defined
          "At most one of of TransformFn<K, V> or TransformFnWithSideInputContext<K, V> may be set");
      if (toFinalResultTWithSides != null) {
        Preconditions.checkNotNull(
            this.sides,
            "If using TransformFnWithSideInputContext<K, V>, side inputs must not be null");
      }
    }

    public AbsCoGbkTransform<KeyType, V> via(TransformFn<KeyType, V> toFinalResultT) {
      this.toFinalResultT = toFinalResultT;
      xformCheck();
      return this;
    }

    public AbsCoGbkTransform<KeyType, V> via(
        TransformFnWithSideInputContext<KeyType, V> toFinalResultTWithSides,
        Iterable<PCollectionView<?>> sides) {
      this.toFinalResultTWithSides = toFinalResultTWithSides;
      this.sides = sides;
      xformCheck();
      return this;
    }

    ResourceId getTempDirectoryOrDefault(Pipeline pipeline) {
      if (tempDirectory != null) return tempDirectory;
      final String tempLocationOpt = pipeline.getOptions().getTempLocation();
      LoggerFactory.getLogger(SortedBucketIO.class)
          .info(
              "tempDirectory was not set for SortedBucketTransform, defaulting to {}",
              tempLocationOpt);
      return FileSystems.matchNewResource(tempLocationOpt, true);
    }

    @Override
    public WriteResult expand(PBegin input) {
      Preconditions.checkNotNull(outputDirectory, "outputDirectory is not set");
      xformCheck();
      final ResourceId tmpDir = getTempDirectoryOrDefault(input.getPipeline());
      return input.apply(
          new SortedBucketTransform<>(
              inputs,
              toKeyFn(),
              comparator(),
              targetParallelism,
              toFinalResultT,
              toFinalResultTWithSides,
              outputDirectory,
              tmpDir,
              sides,
              newBucketMetadataFn,
              fileOperations,
              filenameSuffix,
              filenamePrefix));
    }
  }

  public static class CoGbkTransform<K1, V> extends AbsCoGbkTransform<K1, V> {
    private final Class<K1> keyClassPrimary;
    private Coder<K1> _keyCoderPrimary = null;
    private Function<SortedBucketIO.ComparableKeyBytes, K1> _keyFn = null;
    private final Comparator<SortedBucketIO.ComparableKeyBytes> _comparator =
        new SortedBucketIO.PrimaryKeyComparator();

    private CoGbkTransform(
        Class<K1> keyClassPrimary,
        List<BucketedInput<?>> inputs,
        TargetParallelism targetParallelism,
        TransformOutput<K1, Void, V> transform) {
      super(inputs, targetParallelism, transform);
      this.keyClassPrimary = keyClassPrimary;
    }

    @Override
    protected Comparator<SortedBucketIO.ComparableKeyBytes> comparator() {
      return _comparator;
    }

    @SuppressWarnings("unchecked")
    private Coder<K1> keyTypeCoder() {
      if (_keyCoderPrimary != null) return _keyCoderPrimary;
      Optional<Coder<K1>> c =
          inputs.stream()
              .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
              .filter(sm -> sm.metadata.getKeyClass() == keyClassPrimary)
              .findFirst()
              .map(sm -> (Coder<K1>) sm.metadata.getKeyCoder());
      if (!c.isPresent())
        throw new NullPointerException("Could not infer coder for key class " + keyClassPrimary);
      _keyCoderPrimary = c.get();
      return _keyCoderPrimary;
    }

    @Override
    protected Function<SortedBucketIO.ComparableKeyBytes, K1> toKeyFn() {
      if (_keyFn == null) _keyFn = ComparableKeyBytes.keyFnPrimary(keyTypeCoder());
      return _keyFn;
    }
  }

  public static class CoGbkTransformWithSecondary<K1, K2, V>
      extends AbsCoGbkTransform<KV<K1, K2>, V> {
    private final Class<K1> keyClassPrimary;
    private final Class<K2> keyClassSecondary;
    private Coder<K1> _keyCoderPrimary = null;
    private Coder<K2> _keyCoderSecondary = null;
    private Function<SortedBucketIO.ComparableKeyBytes, KV<K1, K2>> _keyFn = null;
    private final Comparator<SortedBucketIO.ComparableKeyBytes> _comparator =
        new SortedBucketIO.PrimaryAndSecondaryKeyComparator();

    private CoGbkTransformWithSecondary(
        Class<K1> keyClassPrimary,
        Class<K2> keyClassSecondary,
        List<BucketedInput<?>> inputs,
        TargetParallelism targetParallelism,
        TransformOutput<K1, K2, V> transform) {
      super(inputs, targetParallelism, transform);
      this.keyClassPrimary = keyClassPrimary;
      this.keyClassSecondary = keyClassSecondary;
    }

    @Override
    protected Comparator<SortedBucketIO.ComparableKeyBytes> comparator() {
      return _comparator;
    }

    @SuppressWarnings("unchecked")
    private Coder<K1> keyCoderPrimary() {
      if (_keyCoderPrimary != null) return _keyCoderPrimary;
      Optional<Coder<K1>> c =
          inputs.stream()
              .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
              .filter(sm -> sm.metadata.getKeyClass() == keyClassPrimary)
              .findFirst()
              .map(sm -> (Coder<K1>) sm.metadata.getKeyCoder());
      if (!c.isPresent())
        throw new NullPointerException("Could not infer coder for key class " + keyClassPrimary);
      _keyCoderPrimary = c.get();
      return _keyCoderPrimary;
    }

    @SuppressWarnings("unchecked")
    private Coder<K2> keyCoderSecondary() {
      if (_keyCoderSecondary != null) return _keyCoderSecondary;
      Optional<Coder<K2>> c2 =
          inputs.stream()
              .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
              .filter(
                  sm ->
                      sm.metadata.getKeyClassSecondary() != null
                          && sm.metadata.getKeyClassSecondary() == keyClassSecondary
                          && sm.metadata.getKeyCoderSecondary() != null)
              .findFirst()
              .map(sm -> (Coder<K2>) sm.metadata.getKeyCoderSecondary());

      if (!c2.isPresent())
        throw new NullPointerException("Could not infer coder for key class " + keyClassSecondary);
      _keyCoderSecondary = c2.get();
      return _keyCoderSecondary;
    }

    @Override
    protected Function<SortedBucketIO.ComparableKeyBytes, KV<K1, K2>> toKeyFn() {
      if (_keyFn == null)
        _keyFn =
            ComparableKeyBytes.keyFnPrimaryAndSecondary(keyCoderPrimary(), keyCoderSecondary());
      return _keyFn;
    }
  }

  public abstract static class TransformOutput<K1, K2, V> implements Serializable {
    abstract Class<K1> getKeyClassPrimary();

    @Nullable
    abstract Class<K2> getKeyClassSecondary();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract String getFilenamePrefix();

    abstract FileOperations<V> getFileOperations();

    abstract NewBucketMetadataFn<K1, K2, V> getNewBucketMetadataFn();
  }

  /** Represents a single sorted-bucket source written using {@link SortedBucketSink}. */
  public abstract static class Read<V> implements Serializable {
    public abstract TupleTag<V> getTupleTag();

    protected abstract BucketedInput<V> toBucketedInput(SortedBucketSource.Keying keying);
  }

  @FunctionalInterface
  interface KeyFn<K> extends Function<ComparableKeyBytes, K>, Serializable {
    @Override
    K apply(ComparableKeyBytes input);
  }

  /** Must match write keying */
  public static class ComparableKeyBytes {
    private static final Comparator<byte[]> bytesComparator =
        UnsignedBytes.lexicographicalComparator();

    public final byte[] primary;
    public final byte[] secondary;

    public ComparableKeyBytes(byte[] primary) {
      this.primary = primary;
      this.secondary = null;
    }

    public ComparableKeyBytes(byte[] primary, byte[] secondary) {
      this.primary = primary;
      this.secondary = secondary;
    }

    public int comparePrimary(final ComparableKeyBytes o) {
      return bytesComparator.compare(this.primary, o.primary);
    }

    public int comparePrimaryAndSecondary(final ComparableKeyBytes o) {
      int pComp = comparePrimary(o);
      if (pComp != 0) return pComp;
      assert (secondary != null);
      assert (o.secondary != null);
      return bytesComparator.compare(this.secondary, o.secondary);
    }

    public static <K1> KeyFn<K1> keyFnPrimary(final Coder<K1> keyCoderPrimary) {
      return k -> {
        try {
          return keyCoderPrimary.decode(new ByteArrayInputStream(k.primary));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    }

    public static <K1, K2> KeyFn<KV<K1, K2>> keyFnPrimaryAndSecondary(
        final Coder<K1> keyCoderPrimary, final Coder<K2> keyCoderSecondary) {
      return k -> {
        assert (k.secondary != null);
        K1 primary = keyFnPrimary(keyCoderPrimary).apply(k);
        try {
          return KV.of(primary, keyCoderSecondary.decode(new ByteArrayInputStream(k.secondary)));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    }
  }

  /** Comparator only comparing primary keys */
  public static class PrimaryKeyComparator implements Comparator<ComparableKeyBytes>, Serializable {
    @Override
    public int compare(final ComparableKeyBytes o1, final ComparableKeyBytes o2) {
      return o1.comparePrimary(o2);
    }
  }

  /**
   * Comparator comparing both primary and secondary keys
   */
  public static class PrimaryAndSecondaryKeyComparator
      implements Comparator<ComparableKeyBytes>, Serializable {
    @Override
    public int compare(final ComparableKeyBytes o1, final ComparableKeyBytes o2) {
      return o1.comparePrimaryAndSecondary(o2);
    }
  }

  public abstract static class Write<K1, K2, V> extends PTransform<PCollection<V>, WriteResult> {
    @Nullable
    abstract Integer getNumBuckets();

    abstract int getNumShards();

    abstract String getFilenamePrefix();

    abstract Class<K1> getKeyClassPrimary();

    @Nullable
    abstract Class<K2> getKeyClassSecondary();

    abstract HashType getHashType();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract int getSorterMemoryMb();

    abstract FileOperations<V> getFileOperations();

    abstract BucketMetadata<K1, K2, V> getBucketMetadata();

    abstract int getKeyCacheSize();

    public PreKeyedWrite<K1, V> onKeyedCollection(
        Coder<V> valueCoder, boolean verifyKeyExtraction) {
      return new PreKeyedWrite<>(this, valueCoder, verifyKeyExtraction);
    }

    ResourceId getTempDirectoryOrDefault(Pipeline pipeline) {
      if (getTempDirectory() != null) {
        return getTempDirectory();
      }

      final String tempLocationOpt = pipeline.getOptions().getTempLocation();
      LoggerFactory.getLogger(SortedBucketIO.class)
          .info(
              "tempDirectory was not set for SortedBucketSink, defaulting to {}", tempLocationOpt);
      return FileSystems.matchNewResource(tempLocationOpt, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteResult expand(PCollection<V> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");
      Preconditions.checkArgument(
          getNumBuckets() != null && getNumBuckets() > 0,
          "numBuckets must be set to a nonzero value");

      return input.apply(
          new SortedBucketSink<>(
              getBucketMetadata(),
              getOutputDirectory(),
              getTempDirectoryOrDefault(input.getPipeline()),
              getFilenameSuffix(),
              getFileOperations(),
              getSorterMemoryMb(),
              getKeyCacheSize()));
    }
  }

  public static class PreKeyedWrite<K, V> extends PTransform<PCollection<KV<K, V>>, WriteResult> {
    private final Write<K, ?, V> write;
    private final Coder<V> valueCoder;
    private final boolean verifyKeyExtraction;

    public PreKeyedWrite(Write<K, ?, V> write, Coder<V> valueCoder, boolean verifyKeyExtraction) {
      this.write = write;
      this.valueCoder = valueCoder;
      this.verifyKeyExtraction = verifyKeyExtraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteResult expand(PCollection<KV<K, V>> input) {
      Preconditions.checkNotNull(write.getOutputDirectory(), "outputDirectory is not set");
      Preconditions.checkArgument(
          write.getNumBuckets() != null && write.getNumBuckets() > 0,
          "numBuckets must be set to a nonzero value");

      final ResourceId outputDirectory = write.getOutputDirectory();
      ResourceId tempDirectory = write.getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }
      return input.apply(
          new SortedBucketPreKeyedSink<>(
              write.getBucketMetadata(),
              outputDirectory,
              tempDirectory,
              write.getFilenameSuffix(),
              write.getFileOperations(),
              write.getSorterMemoryMb(),
              valueCoder,
              verifyKeyExtraction,
              write.getKeyCacheSize()));
    }
  }
}
