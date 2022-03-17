/*
 * Copyright 2020 Spotify AB.
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

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SourceSpec implements Serializable {
  private static Logger LOG = LoggerFactory.getLogger(SourceSpec.class);

  int leastNumBuckets;
  int greatestNumBuckets;
  HashType hashType;

  private SourceSpec(int leastNumBuckets, int greatestNumBuckets, HashType hashType) {
    this.leastNumBuckets = leastNumBuckets;
    this.greatestNumBuckets = greatestNumBuckets;
    this.hashType = hashType;
  }

  static SourceSpec from(List<SortedBucketSource.BucketedInput<?>> sources) {
    int greatestNumBuckets = 0;
    int leastNumBuckets = Integer.MAX_VALUE;
    BucketMetadata<?, ?, ?> first = null;
    HashType hashType = null;

    for (SortedBucketSource.BucketedInput<?> src : sources) {
      for (BucketMetadataUtil.SourceMetadataValue<?> md :
          src.getSourceMetadata().mapping.values()) {
        final BucketMetadata<?, ?, ?> current = md.metadata;
        if (first == null) {
          first = current;
          hashType = current.getHashType();
        }
        final int buckets = current.getNumBuckets();
        greatestNumBuckets = Math.max(buckets, greatestNumBuckets);
        leastNumBuckets = Math.min(buckets, leastNumBuckets);
        Preconditions.checkState(
            first.isCompatibleWith(current),
            "Source %s is incompatible with source %s",
            sources.get(0),
            src);
      }
    }
    Preconditions.checkNotNull(hashType, "Could not infer hash type for sources");
    return new SourceSpec(leastNumBuckets, greatestNumBuckets, hashType);
  }

  public int getParallelism(TargetParallelism targetParallelism) {
    if (targetParallelism.isMin()) return leastNumBuckets;
    if (targetParallelism.isMax()) return greatestNumBuckets;
    if (targetParallelism.isAuto())
      throw new UnsupportedOperationException("Can't derive a static value for AutoParallelism");
    Preconditions.checkArgument(
        (targetParallelism.getValue() & targetParallelism.getValue() - 1) == 0,
        String.format(
            "Target parallelism must be a power of 2. Was: %d", targetParallelism.getValue()));
    if (targetParallelism.getValue() > greatestNumBuckets)
      LOG.warn(
          String.format(
              "You have selected a parallelism > the greatest number of buckets (%d). Unless you are applying a SortedBucketTransform, consider a lower number.",
              greatestNumBuckets));
    return targetParallelism.getValue();
  }

  @Override
  public String toString() {
    return "SourceSpec{leastNumBuckets="
        + leastNumBuckets
        + ", greatestNumBuckets="
        + greatestNumBuckets
        + '}';
  }
}
