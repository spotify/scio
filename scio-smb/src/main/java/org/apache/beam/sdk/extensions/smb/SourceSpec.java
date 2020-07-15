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
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.PartitionMetadata;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SourceSpec<K> implements Serializable {
  private static Logger LOG = LoggerFactory.getLogger(SourceSpec.class);

  int leastNumBuckets;
  int greatestNumBuckets;
  Coder<K> keyCoder;
  HashType hashType;

  private SourceSpec(
      int leastNumBuckets, int greatestNumBuckets, Coder<K> keyCoder, HashType hashType) {
    this.leastNumBuckets = leastNumBuckets;
    this.greatestNumBuckets = greatestNumBuckets;
    this.keyCoder = keyCoder;
    this.hashType = hashType;
  }

  static <KeyT> SourceSpec<KeyT> from(
      Class<KeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    BucketMetadata<?, ?> first = null;
    Coder<KeyT> finalKeyCoder = null;
    HashType hashType = null;

    // Check metadata of each source
    for (BucketedInput<?, ?> source : sources) {
      final BucketMetadata<?, ?> current = source.getMetadata();
      if (first == null) {
        first = current;
        hashType = current.getHashType();
      } else {
        Preconditions.checkState(
            first.isCompatibleWith(current),
            "Source %s is incompatible with source %s",
            sources.get(0),
            source);
      }

      if (current.getKeyClass() == finalKeyClass && finalKeyCoder == null) {
        try {
          @SuppressWarnings("unchecked")
          final Coder<KeyT> coder = (Coder<KeyT>) current.getKeyCoder();
          finalKeyCoder = coder;
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException("Could not provide coder for key class " + finalKeyClass, e);
        } catch (NonDeterministicException e) {
          throw new RuntimeException("Non-deterministic coder for key class " + finalKeyClass, e);
        }
      }
    }

    int leastNumBuckets =
        sources.stream()
            .flatMap(source -> source.getPartitionMetadata().values().stream())
            .map(PartitionMetadata::getNumBuckets)
            .min(Integer::compareTo)
            .get();

    int greatestNumBuckets =
        sources.stream()
            .flatMap(source -> source.getPartitionMetadata().values().stream())
            .map(PartitionMetadata::getNumBuckets)
            .max(Integer::compareTo)
            .get();

    Preconditions.checkNotNull(
        finalKeyCoder, "Could not infer coder for key class %s", finalKeyClass);

    Preconditions.checkNotNull(hashType, "Could not infer hash type for sources");

    return new SourceSpec<>(leastNumBuckets, greatestNumBuckets, finalKeyCoder, hashType);
  }

  int getParallelism(TargetParallelism targetParallelism) {
    if (targetParallelism.isMin()) {
      return leastNumBuckets;
    } else if (targetParallelism.isMax()) {
      return greatestNumBuckets;
    } else if (targetParallelism.isAuto()) {
      throw new UnsupportedOperationException("Can't derive a static value for AutoParallelism");
    } else {
      Preconditions.checkArgument(
          (targetParallelism.getValue() & targetParallelism.getValue() - 1) == 0,
          String.format(
              "Target parallelism must be a power of 2. Was: %d", targetParallelism.getValue()));

      if (targetParallelism.getValue() > greatestNumBuckets) {
        LOG.warn(
            String.format(
                "You have selected a parallelism > the greatest number of buckets (%d). "
                    + "Unless you are applying a SortedBucketTransform, consider a lower number.",
                greatestNumBuckets));
      }
      return targetParallelism.getValue();
    }
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
