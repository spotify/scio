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
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.PartitionMetadata;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

class SourceSpec<K> implements Serializable {
  int leastNumBuckets;
  int greatestNumBuckets;
  Coder<K> keyCoder;

  private SourceSpec(int leastNumBuckets, int greatestNumBuckets, Coder<K> keyCoder) {
    this.leastNumBuckets = leastNumBuckets;
    this.greatestNumBuckets = greatestNumBuckets;
    this.keyCoder = keyCoder;
  }

  static <KeyT> SourceSpec<KeyT> from(
      Class<KeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    BucketMetadata<?, ?> first = null;
    Coder<KeyT> finalKeyCoder = null;

    // Check metadata of each source
    for (BucketedInput<?, ?> source : sources) {
      final BucketMetadata<?, ?> current = source.getMetadata();
      if (first == null) {
        first = current;
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

    return new SourceSpec<>(leastNumBuckets, greatestNumBuckets, finalKeyCoder);
  }

  int getParallelism(TargetParallelism targetParallelism) {
    int parallelism;

    if (targetParallelism.isMin()) {
      return leastNumBuckets;
    } else if (targetParallelism.isMax()) {
      return greatestNumBuckets;
    } else if (targetParallelism.isAuto()) {
      throw new UnsupportedOperationException("Can't derive a static value for AutoParallelism");
    } else {
      parallelism = targetParallelism.getValue();

      Preconditions.checkArgument(
          ((parallelism & parallelism - 1) == 0)
              && parallelism >= leastNumBuckets
              && parallelism <= greatestNumBuckets,
          String.format(
              "Target parallelism must be a power of 2 between the least (%d) and "
                  + "greatest (%d) number of buckets in sources. Was: %d",
              leastNumBuckets, greatestNumBuckets, parallelism));

      return parallelism;
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
