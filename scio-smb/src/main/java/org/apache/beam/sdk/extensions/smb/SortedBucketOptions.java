/*
 * Copyright 2021 Spotify AB.
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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Description("Tuning options for SortMergeBucket")
public interface SortedBucketOptions extends PipelineOptions {
  @Description("Buffer size for each SMB source file, in number of elements")
  @Default.Integer(10000)
  int getSortedBucketReadBufferSize();

  void setSortedBucketReadBufferSize(int readBufferSize);

  @Description("Buffer size for all SMB source files on worker disk, in MiB")
  @Default.Integer(0)
  int getSortedBucketReadDiskBufferMb();

  void setSortedBucketReadDiskBufferMb(int readDiskBufferMb);

  @Description(
      "Factor to multiply the runner-provided `desiredBundleSizeBytes` during SMB reads. A lower value "
          + "results in more generated splits, which can improve scaling at read-time.")
  @Default.Double(0.5D)
  double getSortedBucketSplitAdjustmentFactor();

  void setSortedBucketSplitAdjustmentFactor(double sortedBucketSplitAdjustmentFactor);
}
