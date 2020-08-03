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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

public class BucketMetadataUtil {
  private static final int BATCH_SIZE = 100;
  private static final BucketMetadataUtil INSTANCE = new BucketMetadataUtil(BATCH_SIZE);
  private final int batchSize;

  public static BucketMetadataUtil get() {
    return INSTANCE;
  }

  ////////////////////////////////////////////////////////////////////////////////

  @AutoValue
  public abstract static class SourceMetadata<K, V> implements Serializable {
    public static <K, V> SourceMetadata<K, V> create(
        @Nullable BucketMetadata<K, V> canonicalMetadata,
        Map<ResourceId, PartitionMetadata> partitionMetadata) {
      return new AutoValue_BucketMetadataUtil_SourceMetadata<>(
          canonicalMetadata, partitionMetadata);
    }

    @Nullable
    public abstract BucketMetadata<K, V> getCanonicalMetadata();

    public abstract Map<ResourceId, PartitionMetadata> getPartitionMetadata();

    public boolean supportsSmb() {
      return getCanonicalMetadata() != null && !getPartitionMetadata().isEmpty();
    }
  }

  @AutoValue
  public abstract static class PartitionMetadata implements Serializable {
    public static PartitionMetadata create(
        FileAssignment fileAssignment, int numBuckets, int numShards) {
      return new AutoValue_BucketMetadataUtil_PartitionMetadata(
          fileAssignment, numBuckets, numShards);
    }

    public abstract FileAssignment getFileAssignment();

    public abstract int getNumBuckets();

    public abstract int getNumShards();
  }

  ////////////////////////////////////////////////////////////////////////////////

  @VisibleForTesting
  BucketMetadataUtil(int batchSize) {
    this.batchSize = batchSize;
  }

  public <K, V> SourceMetadata<K, V> getSourceMetadata(
      List<ResourceId> directories, String filenameSuffix) {
    final int total = directories.size();
    final Map<ResourceId, PartitionMetadata> partitionMetadata = new HashMap<>();
    BucketMetadata<K, V> canonicalMetadata = null;
    ResourceId canonicalMetadataDir = null;
    int start = 0;
    while (start < total) {
      final List<ResourceId> input = directories.subList(start, Math.min(total, start + batchSize));
      final List<Optional<BucketMetadata<K, V>>> result =
          input
              .parallelStream()
              .map(BucketMetadataUtil::<K, V>getMetadata)
              .collect(Collectors.toList());

      if (result.stream().anyMatch(o -> !o.isPresent())) {
        // Fail fast if any partition is missing metadata
        return SourceMetadata.create(null, Collections.emptyMap());
      }

      for (int i = 0; i < result.size(); i++) {
        final BucketMetadata<K, V> metadata = result.get(i).get();
        final ResourceId dir = input.get(i);
        final FileAssignment fileAssignment =
            new SMBFilenamePolicy(dir, metadata.getFilenamePrefix(), filenameSuffix)
                .forDestination();

        if (canonicalMetadata == null) {
          canonicalMetadata = metadata;
          canonicalMetadataDir = dir;
        }

        Preconditions.checkState(
            metadata.isCompatibleWith(canonicalMetadata)
                && metadata.isPartitionCompatible(canonicalMetadata),
            "Incompatible partitions. Metadata %s is incompatible with metadata %s. %s != %s",
            dir,
            canonicalMetadataDir,
            metadata,
            canonicalMetadata);

        if (metadata.getNumBuckets() < canonicalMetadata.getNumBuckets()) {
          canonicalMetadata = metadata;
          canonicalMetadataDir = dir;
        }

        final PartitionMetadata value =
            PartitionMetadata.create(
                fileAssignment, metadata.getNumBuckets(), metadata.getNumShards());
        partitionMetadata.put(dir, value);
      }

      start += batchSize;
    }
    return SourceMetadata.create(canonicalMetadata, partitionMetadata);
  }

  ////////////////////////////////////////////////////////////////////////////////

  private static FileAssignment getFileAssignment(
      ResourceId directory, String filenamePrefix, String filenameSuffix) {
    return new SMBFilenamePolicy(directory, filenamePrefix, filenameSuffix).forDestination();
  }

  private static <K, V> Optional<BucketMetadata<K, V>> getMetadata(ResourceId directory) {
    final ResourceId resourceId = FileAssignment.forDstMetadata(directory);
    try {
      InputStream inputStream = Channels.newInputStream(FileSystems.open(resourceId));
      return Optional.of(BucketMetadata.from(inputStream));
    } catch (FileNotFoundException e) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException("Error fetching bucket metadata " + resourceId, e);
    }
  }
}
