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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
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
  public void testSortedBucketTransformNoFanout() throws Exception {
    final TestBucketMetadata outputMetadata = TestBucketMetadata.of(2, 1);

    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            outputMetadata,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            ".txt",
            new TestFileOperations(),
            sources,
            mergeFunction));

    final PipelineResult result = transformPipeline.run();
    result.waitUntilFinish();

    final KV<BucketMetadata, Set<String>> outputs = readAllFrom(outputFolder, outputMetadata);
    Assert.assertEquals(expected, outputs.getValue());
    Assert.assertEquals(outputMetadata, outputs.getKey());

    verifyMetrics(
        result,
        ImmutableMap.of(
            "SortedBucketTransform-ElementsWritten", 3L,
            "SortedBucketTransform-ElementsRead", 10L),
        ImmutableMap.of(
            "SortedBucketTransform-KeyGroupSize", DistributionResult.create(10, 7, 1, 2)));
  }

  @Test
  public void testWorksWithBucketFanout() throws Exception {
    final TestBucketMetadata outputMetadata = TestBucketMetadata.of(8, 1);

    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            outputMetadata,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            ".txt",
            new TestFileOperations(),
            sources,
            mergeFunction));

    final PipelineResult result = transformPipeline.run();
    result.waitUntilFinish();

    final KV<BucketMetadata, Set<String>> outputs = readAllFrom(outputFolder, outputMetadata);
    Assert.assertEquals(expected, outputs.getValue());
    Assert.assertEquals(outputMetadata, outputs.getKey());

    verifyMetrics(
        result,
        ImmutableMap.of(
            "SortedBucketTransform-ElementsWritten", 3L,
            "SortedBucketTransform-ElementsRead", 10L),
        ImmutableMap.of(
            "SortedBucketTransform-KeyGroupSize", DistributionResult.create(10, 7, 1, 2)));
  }

  private static KV<BucketMetadata, Set<String>> readAllFrom(
      TemporaryFolder folder, TestBucketMetadata metadata) throws Exception {
    final FileAssignment fileAssignment =
        new SMBFilenamePolicy(fromFolder(folder), ".txt").forDestination();

    final Set<String> outputElements = new HashSet<>();

    for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
      final FileOperations.Reader<String> outputReader = new TestFileOperations().createReader();
      outputReader.prepareRead(
          FileSystems.open(fileAssignment.forBucket(BucketShardId.of(bucketId, 0), metadata)));

      outputReader.iterator().forEachRemaining(outputElements::add);
    }

    return KV.of(
        BucketMetadata.from(
            Channels.newInputStream(FileSystems.open(fileAssignment.forMetadata()))),
        outputElements);
  }

  private static void verifyMetrics(
      PipelineResult result,
      Map<String, Long> expectedCounters,
      Map<String, DistributionResult> expectedDistributions) {
    final Map<String, Long> actualCounters =
        ImmutableList.copyOf(result.metrics().allMetrics().getCounters().iterator()).stream()
            .filter(metric -> !metric.getName().getName().equals(PAssert.SUCCESS_COUNTER))
            .collect(
                Collectors.toMap(metric -> metric.getName().getName(), MetricResult::getCommitted));

    Assert.assertEquals(expectedCounters, actualCounters);

    final Map<String, DistributionResult> actualDistributions =
        ImmutableList.copyOf(result.metrics().allMetrics().getDistributions().iterator()).stream()
            .collect(
                Collectors.toMap(metric -> metric.getName().getName(), MetricResult::getCommitted));

    Assert.assertEquals(expectedDistributions, actualDistributions);
  }
}
