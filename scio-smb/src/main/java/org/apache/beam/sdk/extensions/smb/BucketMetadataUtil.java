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

import static com.google.common.base.Verify.verify;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

public class BucketMetadataUtil {
  private static final int BATCH_SIZE = 100;
  private static final BucketMetadataUtil INSTANCE = new BucketMetadataUtil(BATCH_SIZE);
  private final int batchSize;

  public static BucketMetadataUtil get() {
    return INSTANCE;
  }

  public static class SourceMetadataValue<V> {
    public final BucketMetadata<?, ?, V> metadata;
    public final FileAssignment fileAssignment;

    SourceMetadataValue(BucketMetadata<?, ?, V> metadata, FileAssignment fileAssignment) {
      this.metadata = metadata;
      this.fileAssignment = fileAssignment;
    }
  }

  // just a wrapper class for clarity
  public static class SourceMetadata<V> {
    public final Map<ResourceId, SourceMetadataValue<V>> mapping;

    SourceMetadata(Map<ResourceId, SourceMetadataValue<V>> mapping) {
      verify(!mapping.isEmpty());
      this.mapping = mapping;
    }

    /** @return smallest number of buckets for this set of inputs. */
    int leastNumBuckets() {
      return mapping.values().stream().mapToInt(v -> v.metadata.getNumBuckets()).min().getAsInt();
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  @VisibleForTesting
  BucketMetadataUtil(int batchSize) {
    this.batchSize = batchSize;
  }

  private <V> Map<ResourceId, BucketMetadata<?, ?, V>> fetchMetadata(List<ResourceId> directories) {
    final int total = directories.size();
    final Map<ResourceId, BucketMetadata<?, ?, V>> metadata = new ConcurrentHashMap<>();
    int start = 0;
    while (start < total) {
      directories.stream()
          .skip(start)
          .limit(batchSize)
          .parallel()
          .forEach(dir -> metadata.put(dir, BucketMetadata.get(dir)));
      start += batchSize;
    }
    return metadata;
  }

  private <V> SourceMetadata<V> getSourceMetadata(
      List<ResourceId> directories,
      String filenameSuffix,
      BiFunction<BucketMetadata<?, ?, V>, BucketMetadata<?, ?, V>, Boolean>
          compatibilityCompareFn) {
    final Map<ResourceId, BucketMetadata<?, ?, V>> bucketMetadatas = fetchMetadata(directories);
    Preconditions.checkState(!bucketMetadatas.isEmpty(), "Failed to find metadata");

    Map<ResourceId, SourceMetadataValue<V>> mapping = new HashMap<>();
    Map.Entry<ResourceId, BucketMetadata<?, ?, V>> first =
        bucketMetadatas.entrySet().stream().findAny().get();
    bucketMetadatas.forEach(
        (dir, metadata) -> {
          Preconditions.checkState(
              metadata.isCompatibleWith(first.getValue())
                  && compatibilityCompareFn.apply(metadata, first.getValue()),
              "Incompatible partitions. Metadata %s is incompatible with metadata %s. %s != %s",
              dir,
              first.getKey(),
              metadata,
              first.getValue());
          final FileAssignment fileAssignment =
              new SMBFilenamePolicy(dir, metadata.getFilenamePrefix(), filenameSuffix)
                  .forDestination();
          mapping.put(dir, new SourceMetadataValue<>(metadata, fileAssignment));
        });
    return new SourceMetadata<>(mapping);
  }

  public <V> SourceMetadata<V> getPrimaryKeyedSourceMetadata(
      List<ResourceId> directories, String filenameSuffix) {
    return getSourceMetadata(
        directories, filenameSuffix, BucketMetadata::isPartitionCompatibleForPrimaryKey);
  }

  public <V> SourceMetadata<V> getPrimaryAndSecondaryKeyedSourceMetadata(
      List<ResourceId> directories, String filenameSuffix) {
    return getSourceMetadata(
        directories,
        filenameSuffix,
        BucketMetadata::isPartitionCompatibleForPrimaryAndSecondaryKey);
  }
}
