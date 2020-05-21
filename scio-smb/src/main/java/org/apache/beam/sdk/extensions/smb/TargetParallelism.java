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

/**
 * Represents the desired parallelism of an SMB read operation. For a given set of sources,
 * targetParallelism can be set to any number between the least and greatest numbers of buckets
 * among sources. This can be dynamically configured using {@link TargetParallelism#min()} or {@link
 * TargetParallelism#max()}, which at graph construction time will determine the least or greatest
 * amount of parallelism based on sources. Alternately, {@link TargetParallelism#of(int)} can be
 * used to statically configure a custom value.
 *
 * <p>If no value is specified, SMB read operations will use the minimum parallelism.
 *
 * <p>When selecting a target parallelism for your SMB operation, there are tradeoffs to consider:
 *
 * <p>- Minimal parallelism means a fewer number of workers merging data from potentially many
 * buckets. For example, if source A has 4 buckets and source B has 64, a minimally parallel SMB
 * read would have 4 workers, each one merging 1 bucket from source A and 16 buckets from source B.
 * This read may have low throughput. - Maximal parallelism means that each bucket is read by at
 * least one worker. For example, if source A has 4 buckets and source B has 64, a maximally
 * parallel SMB read would have 64 workers, each one merging 1 bucket from source B and 1 bucket
 * from source A, replicated 16 times. This may have better throughput than the minimal example, but
 * more expensive because every key group from the replicated sources must be re-hashed to avoid
 * emitting duplicate records. - A custom parallelism in the middle of these bounds may be the best
 * balance of speed and computing cost.
 */
public abstract class TargetParallelism implements Serializable {

  public static MinParallelism min() {
    return MinParallelism.INSTANCE;
  }

  public static MaxParallelism max() {
    return MaxParallelism.INSTANCE;
  }

  public static AutoParallelism auto() {
    return AutoParallelism.INSTANCE;
  }

  public static CustomParallelism of(int value) {
    return new CustomParallelism(value);
  }

  boolean isMax() {
    return this.getClass().equals(MaxParallelism.class);
  }

  boolean isMin() {
    return this.getClass().equals(MinParallelism.class);
  }

  boolean isCustom() {
    return this.getClass().equals(CustomParallelism.class);
  }

  boolean isAuto() {
    return this.getClass().equals(AutoParallelism.class);
  }

  abstract int getValue();

  static class MaxParallelism extends TargetParallelism {
    static MaxParallelism INSTANCE = new MaxParallelism();

    private MaxParallelism() {}

    @Override
    public String toString() {
      return "MaxParallelism";
    }

    @Override
    int getValue() {
      throw new UnsupportedOperationException();
    }
  }

  static class MinParallelism extends TargetParallelism {
    static MinParallelism INSTANCE = new MinParallelism();

    private MinParallelism() {}

    @Override
    public String toString() {
      return "MinParallelism";
    }

    @Override
    int getValue() {
      throw new UnsupportedOperationException();
    }
  }

  static class CustomParallelism extends TargetParallelism {
    private int value;

    CustomParallelism(int value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return "CustomParallelism (" + value + ")";
    }

    @Override
    int getValue() {
      return value;
    }
  }

  static class AutoParallelism extends TargetParallelism {
    static AutoParallelism INSTANCE = new AutoParallelism();

    AutoParallelism() {}

    @Override
    public String toString() {
      return "AutoParallelism";
    }

    @Override
    int getValue() {
      throw new UnsupportedOperationException();
    }
  }
}
