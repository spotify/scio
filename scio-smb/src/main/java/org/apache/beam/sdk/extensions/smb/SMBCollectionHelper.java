/*
 * Copyright 2024 Spotify AB.
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

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * Internal adapter for Scio SMBCollection implementation.
 *
 * <p>This class exposes package-private SMB methods to the Scala SMBCollection API. It provides
 * bridge methods to access internal Beam SMB classes that would otherwise be inaccessible from the
 * com.spotify.scio.smb package.
 *
 * <p><b>DO NOT USE - This is an internal implementation detail subject to change without
 * notice.</b>
 *
 * @hidden
 */
@org.apache.beam.sdk.annotations.Internal
public class SMBCollectionHelper {

  /** Get FileOperations from a TransformOutput. */
  public static <K1, K2, V> FileOperations<V> getFileOperations(
      SortedBucketIO.TransformOutput<K1, K2, V> output) {
    return output.getFileOperations();
  }

  /** Get output directory from a TransformOutput. */
  public static <K1, K2, V> ResourceId getOutputDirectory(
      SortedBucketIO.TransformOutput<K1, K2, V> output) {
    return output.getOutputDirectory();
  }

  /** Get temp directory from a TransformOutput. */
  public static <K1, K2, V> ResourceId getTempDirectory(
      SortedBucketIO.TransformOutput<K1, K2, V> output) {
    return output.getTempDirectory();
  }

  /** Get temp directory with fallback to default. */
  public static <K1, K2, V> ResourceId getTempDirectoryOrDefault(
      SortedBucketIO.TransformOutput<K1, K2, V> output, Pipeline pipeline) {
    ResourceId tempDir = output.getTempDirectory();
    if (tempDir != null) {
      return tempDir;
    }
    // Use the same fallback logic as SortedBucketIO.Write
    String tempLocation = pipeline.getOptions().getTempLocation();
    return org.apache.beam.sdk.io.FileSystems.matchNewResource(tempLocation, true);
  }

  /** Get filename prefix from a TransformOutput. */
  public static <K1, K2, V> String getFilenamePrefix(
      SortedBucketIO.TransformOutput<K1, K2, V> output) {
    return output.getFilenamePrefix();
  }

  /** Get filename suffix from a TransformOutput. */
  public static <K1, K2, V> String getFilenameSuffix(
      SortedBucketIO.TransformOutput<K1, K2, V> output) {
    return output.getFilenameSuffix();
  }

  /** Get FileAssignment for temp files from an SMBFilenamePolicy. */
  public static SMBFilenamePolicy.FileAssignment forTempFiles(
      SMBFilenamePolicy policy, ResourceId tempDirectory) {
    return policy.forTempFiles(tempDirectory);
  }

  /** Get ResourceId for a bucket from FileAssignment (exposes package-private forBucket). */
  public static ResourceId forBucket(
      SMBFilenamePolicy.FileAssignment fileAssignment,
      BucketShardId id,
      int maxNumBuckets,
      int maxNumShards) {
    return fileAssignment.forBucket(id, maxNumBuckets, maxNumShards);
  }

  /** Get bucketOffsetId from BucketItem. */
  public static int getBucketOffsetId(SortedBucketTransform.BucketItem item) {
    return item.bucketOffsetId;
  }

  /** Get effectiveParallelism from BucketItem. */
  public static int getEffectiveParallelism(SortedBucketTransform.BucketItem item) {
    return item.effectiveParallelism;
  }

  /** Create SourceSpec from BucketedInputs (exposes package-private class). */
  public static SourceSpec createSourceSpec(
      java.util.List<SortedBucketSource.BucketedInput<?>> inputs) {
    return SourceSpec.from(inputs);
  }

  /** Create BucketSource (exposes package-private class). */
  public static <K> SortedBucketTransform.BucketSource<K> createBucketSource(
      java.util.List<SortedBucketSource.BucketedInput<?>> inputs,
      TargetParallelism targetParallelism,
      int numShards,
      int bucketOffset,
      SourceSpec sourceSpec,
      int keyCacheSize) {
    return new SortedBucketTransform.BucketSource<>(
        inputs, targetParallelism, numShards, bucketOffset, sourceSpec, keyCacheSize);
  }

  /** Check if an Iterable is a TraversableOnceIterable and exhaust it if so. */
  public static void exhaustIfTraversableOnce(Iterable<?> iterable) {
    if (iterable instanceof SortedBucketSource.TraversableOnceIterable) {
      ((SortedBucketSource.TraversableOnceIterable<?>) iterable).ensureExhausted();
    }
  }

  /** Get primary key coder from BucketMetadata. */
  public static org.apache.beam.sdk.coders.Coder<?> getPrimaryKeyCoder(
      BucketMetadata<?, ?, ?> metadata) {
    return metadata.getKeyCoder();
  }

  /** Get secondary key coder from BucketMetadata. */
  public static org.apache.beam.sdk.coders.Coder<?> getSecondaryKeyCoder(
      BucketMetadata<?, ?, ?> metadata) {
    return metadata.getKeyCoderSecondary();
  }

  /**
   * Extract primary key coder from SMB metadata using keyClassMatches. This searches all
   * BucketedInput sources for a metadata with matching primary key class.
   */
  public static <K1> Coder<K1> getPrimaryKeyCoder(
      List<SortedBucketSource.BucketedInput<?>> inputs, Class<K1> keyClass) {
    Optional<Coder<K1>> coder =
        inputs.stream()
            .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
            .filter(sm -> sm.metadata.keyClassMatches(keyClass))
            .findFirst()
            .map(sm -> (Coder<K1>) sm.metadata.getKeyCoder());

    return coder.orElseThrow(
        () ->
            new IllegalStateException(
                "Could not infer key coder for class " + keyClass + " from SMB metadata"));
  }

  /**
   * Extract secondary key coder from SMB metadata using keyClassSecondaryMatches. This searches all
   * BucketedInput sources for a metadata with matching secondary key class.
   */
  public static <K2> Coder<K2> getSecondaryKeyCoder(
      List<SortedBucketSource.BucketedInput<?>> inputs, Class<K2> keyClassSecondary) {
    Optional<Coder<K2>> coder =
        inputs.stream()
            .flatMap(i -> i.getSourceMetadata().mapping.values().stream())
            .filter(
                sm ->
                    sm.metadata.getKeyClassSecondary() != null
                        && sm.metadata.keyClassSecondaryMatches(keyClassSecondary)
                        && sm.metadata.getKeyCoderSecondary() != null)
            .findFirst()
            .map(sm -> (Coder<K2>) sm.metadata.getKeyCoderSecondary());

    return coder.orElseThrow(
        () ->
            new IllegalStateException(
                "Could not infer secondary key coder for class "
                    + keyClassSecondary
                    + " from SMB metadata"));
  }

  /** Create RenameBuckets transform for finalizing bucket files. */
  public static <V>
      org.apache.beam.sdk.transforms.PTransform<
              org.apache.beam.sdk.values.PCollection<
                  org.apache.beam.sdk.values.KV<
                      BucketShardId, org.apache.beam.sdk.io.fs.ResourceId>>,
              org.apache.beam.sdk.values.PCollectionTuple>
          createRenameBuckets(
              org.apache.beam.sdk.io.fs.ResourceId tempDirectory,
              SMBFilenamePolicy.FileAssignment fileAssignment,
              BucketMetadata<?, ?, ?> bucketMetadata,
              FileOperations<V> fileOperations) {
    return new SortedBucketSink.RenameBuckets<>(
        tempDirectory, fileAssignment, bucketMetadata, fileOperations);
  }

  /** Convert PCollectionTuple to WriteResult. */
  public static SortedBucketSink.WriteResult writeResultFromTuple(
      org.apache.beam.sdk.values.PCollectionTuple tuple) {
    return SortedBucketSink.WriteResult.fromTuple(tuple);
  }

  /** Get the coder for bucket files (KV<BucketShardId, ResourceId>). */
  public static org.apache.beam.sdk.coders.Coder<
          org.apache.beam.sdk.values.KV<BucketShardId, org.apache.beam.sdk.io.fs.ResourceId>>
      getBucketFilesCoder() {
    return org.apache.beam.sdk.coders.KvCoder.of(
        org.apache.beam.sdk.extensions.avro.coders.AvroCoder.of(BucketShardId.class),
        org.apache.beam.sdk.io.fs.ResourceIdCoder.of());
  }

  /** Get TupleTag from a SortedBucketIO.Read (used for source metadata extraction). */
  public static org.apache.beam.sdk.values.TupleTag<?> getTupleTag(SortedBucketIO.Read<?> read) {
    return read.getTupleTag(); // Public method
  }

  /**
   * Get Schema from an AvroSortedBucketIO.Read or ParquetAvroSortedBucketIO.Read (used for source
   * metadata extraction).
   */
  public static org.apache.avro.Schema getSchema(SortedBucketIO.Read<?> read) {
    if (read instanceof AvroSortedBucketIO.Read) {
      return ((AvroSortedBucketIO.Read<?>) read).getSchema();
    }
    if (read instanceof ParquetAvroSortedBucketIO.Read) {
      ParquetAvroSortedBucketIO.Read<?> parquetRead = (ParquetAvroSortedBucketIO.Read<?>) read;
      // Try schema first, then derive from record class if needed
      org.apache.avro.Schema schema = parquetRead.getSchema();
      if (schema != null) {
        return schema;
      }
      // Derive schema from record class
      Class<?> recordClass = parquetRead.getRecordClass();
      if (recordClass != null) {
        try {
          return ((org.apache.avro.specific.SpecificRecord)
                  recordClass.getDeclaredConstructor().newInstance())
              .getSchema();
        } catch (Exception e) {
          throw new RuntimeException("Failed to derive schema from record class " + recordClass, e);
        }
      }
      throw new IllegalArgumentException(
          "ParquetAvroSortedBucketIO.Read must have either schema or recordClass");
    }
    throw new IllegalArgumentException(
        "Only AvroSortedBucketIO.Read and ParquetAvroSortedBucketIO.Read are currently supported");
  }

  /**
   * Get input directories from an AvroSortedBucketIO.Read or ParquetAvroSortedBucketIO.Read (used
   * for source metadata extraction).
   */
  public static java.util.List<String> getInputDirectories(SortedBucketIO.Read<?> read) {
    if (read instanceof AvroSortedBucketIO.Read) {
      return ((AvroSortedBucketIO.Read<?>) read).getInputDirectories();
    }
    if (read instanceof ParquetAvroSortedBucketIO.Read) {
      return ((ParquetAvroSortedBucketIO.Read<?>) read).getInputDirectories();
    }
    throw new IllegalArgumentException(
        "Only AvroSortedBucketIO.Read and ParquetAvroSortedBucketIO.Read are currently supported");
  }
}
