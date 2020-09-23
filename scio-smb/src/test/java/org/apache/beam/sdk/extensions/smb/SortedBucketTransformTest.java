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

import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSinkTest.SerializableConsumer;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link SortedBucketTransform}. */
public class SortedBucketTransformTest {
  @ClassRule public static final TestPipeline sinkPipeline = TestPipeline.create();
  @ClassRule public static final TemporaryFolder inputLhsFolder = new TemporaryFolder();
  @ClassRule public static final TemporaryFolder inputRhsFolder = new TemporaryFolder();
  @ClassRule public static final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public final TestPipeline transformPipeline = TestPipeline.create();
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();

  private static final List<String> inputLhs = ImmutableList.of("a1", "b1", "c1", "d1", "e1");
  private static final List<String> inputRhs = ImmutableList.of("c2", "d2", "e2", "f2", "g2");
  private static final Set<String> expected = ImmutableSet.of("c1-c2", "d1-d2", "e1-e2");

  private static List<BucketedInput<?, ?>> sources;

  private static final TransformFn<String, String> mergeFunction =
      (keyGroup, outputConsumer) ->
          keyGroup
              .getValue()
              .getAll(new TupleTag<String>("lhs"))
              .forEach(
                  lhs -> {
                    keyGroup
                        .getValue()
                        .getAll(new TupleTag<String>("rhs"))
                        .forEach(
                            rhs -> {
                              outputConsumer.accept(lhs + "-" + rhs);
                            });
                  });

  @BeforeClass
  public static void writeData() throws Exception {
    sinkPipeline
        .apply("CreateLHS", Create.of(inputLhs))
        .apply(
            "SinkLHS",
            new SortedBucketSink<>(
                TestBucketMetadata.of(4, 3),
                fromFolder(inputLhsFolder),
                fromFolder(tempFolder),
                ".txt",
                new TestFileOperations(),
                1));

    sinkPipeline
        .apply("CreateRHS", Create.of(inputRhs))
        .apply(
            "SinkRHS",
            new SortedBucketSink<>(
                TestBucketMetadata.of(2, 1),
                fromFolder(inputRhsFolder),
                fromFolder(tempFolder),
                ".txt",
                new TestFileOperations(),
                1));

    sinkPipeline.run().waitUntilFinish();

    sources =
        ImmutableList.of(
            new BucketedInput<String, String>(
                new TupleTag<>("lhs"),
                fromFolder(inputLhsFolder),
                ".txt",
                new TestFileOperations()),
            new BucketedInput<String, String>(
                new TupleTag<>("rhs"),
                fromFolder(inputRhsFolder),
                ".txt",
                new TestFileOperations()));
  }

  @Test
  public void testSortedBucketTransformMinParallelism() throws Exception {
    test(TargetParallelism.min(), 2);
  }

  @Test
  public void testSortedBucketTransformMaxParallelism() throws Exception {
    test(TargetParallelism.max(), 4);
  }

  @Test
  public void testSortedBucketTransformAutoParallelism() throws Exception {
    test(TargetParallelism.auto(), -1);
  }

  @Test
  public void testSortedBucketTransformCustomParallelism() throws Exception {
    test(TargetParallelism.of(8), 8);
  }

  private void test(TargetParallelism targetParallelism, int expectedNumBuckets) throws Exception {
    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            sources,
            targetParallelism,
            mergeFunction,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            (numBuckets, numShards, hashType) -> TestBucketMetadata.of(numBuckets, numShards),
            new TestFileOperations(),
            ".txt",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX));

    final PipelineResult result = transformPipeline.run();
    result.waitUntilFinish();

    final KV<TestBucketMetadata, Map<BucketShardId, List<String>>> outputs =
        readAllFrom(outputFolder);
    int numBucketsInMetadata = outputs.getKey().getNumBuckets();

    if (!targetParallelism.isAuto()) {
      Assert.assertEquals(expectedNumBuckets, numBucketsInMetadata);
    } else {
      Assert.assertTrue(numBucketsInMetadata <= 4);
      Assert.assertTrue(numBucketsInMetadata >= 1);
    }

    SortedBucketSinkTest.assertValidSmbFormat(outputs.getKey(), expected.toArray(new String[0]))
        .accept(outputs.getValue());

    Assert.assertEquals(1, outputs.getKey().getNumShards());

    SortedBucketSourceTest.verifyMetrics(
        result,
        ImmutableMap.of(
            "SortedBucketTransform-KeyGroupSize", DistributionResult.create(10, 7, 1, 2)));
  }

  private static KV<TestBucketMetadata, Map<BucketShardId, List<String>>> readAllFrom(
      TemporaryFolder folder) throws Exception {
    final FileAssignment fileAssignment =
        new SMBFilenamePolicy(fromFolder(folder), SortedBucketIO.DEFAULT_FILENAME_PREFIX, ".txt")
            .forDestination();

    BucketMetadata<String, String> metadata =
        BucketMetadata.from(
            Channels.newInputStream(FileSystems.open(fileAssignment.forMetadata())));

    final Map<BucketShardId, List<String>> bucketsToOutputs = new HashMap<>();

    for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
      final FileOperations.Reader<String> outputReader = new TestFileOperations().createReader();
      outputReader.prepareRead(
          FileSystems.open(fileAssignment.forBucket(BucketShardId.of(bucketId, 0), metadata)));

      bucketsToOutputs.put(
          BucketShardId.of(bucketId, 0), Lists.newArrayList(outputReader.iterator()));
    }

    return KV.of((TestBucketMetadata) metadata, bucketsToOutputs);
  }
}
