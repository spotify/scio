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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate;
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
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

  private static final List<String> inputLhs = ImmutableList.of("", "a1", "b1", "c1", "d1", "e1");
  private static final List<String> inputRhs =
      ImmutableList.of("", "c2", "d2", "e2", "f2", "g2", "h2");
  private static final List<Integer> inputSI = ImmutableList.of(1, 2, 3, 4, 5, 6);
  private static final List<String> inputSI2 = ImmutableList.of("z", "x", "y");

  // Predicate will filter out c2 from RHS input
  private static final Set<String> expected = ImmutableSet.of("d1-d2", "e1-e2");
  private static final Set<String> expectedWithSides =
      ImmutableSet.of("d1-d2-1,2,3,4,5,6-x,y,z", "e1-e2-1,2,3,4,5,6-x,y,z");

  private static List<BucketedInput<?, ?>> sources;

  private static final TransformFn<String, String> mergeFunction =
      (keyGroup, outputConsumer) ->
          keyGroup
              .getValue()
              .getAll(new TupleTag<String>("lhs"))
              .forEach(
                  lhs ->
                      keyGroup
                          .getValue()
                          .getAll(new TupleTag<String>("rhs"))
                          .forEach(rhs -> outputConsumer.accept(lhs + "-" + rhs)));

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

    final Predicate<String> predicate = (xs, s) -> !s.startsWith("c") && !s.startsWith("h");

    sources =
        ImmutableList.of(
            new BucketedInput<String, String>(
                new TupleTag<>("lhs"),
                inputLhsFolder.getRoot().getAbsolutePath(),
                ".txt",
                new TestFileOperations()),
            new BucketedInput<String, String>(
                new TupleTag<>("rhs"),
                Collections.singletonList(inputRhsFolder.getRoot().getAbsolutePath()),
                ".txt",
                new TestFileOperations(),
                predicate));
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

  @Test
  public void testSortedBucketWithSidesTransformMinParallelism() throws Exception {
    testWithSides(TargetParallelism.min(), 2);
  }

  @Test
  public void testSortedBucketWithSidesTransformMaxParallelism() throws Exception {
    testWithSides(TargetParallelism.max(), 4);
  }

  @Test
  public void testSortedBucketWithSidesTransformAutoParallelism() throws Exception {
    testWithSides(TargetParallelism.auto(), -1);
  }

  @Test
  public void testSortedBucketWithSidesTransformCustomParallelism() throws Exception {
    testWithSides(TargetParallelism.of(8), 8);
  }

  private void testWithSides(TargetParallelism targetParallelism, int expectedNumBuckets)
      throws Exception {
    final PCollectionView<List<Integer>> ints =
        transformPipeline.apply("CreateSI", Create.of(inputSI)).apply("SI", View.asList());
    final PCollectionView<List<String>> chars =
        transformPipeline.apply("CreateSI2", Create.of(inputSI2)).apply("SI1", View.asList());

    final SortedBucketTransform.TransformFnWithSideInputContext<String, String> sideMergeFunction =
        (keyGroup, ctx, outputConsumer, window) -> {
          List<String> si =
              ctx.sideInput(ints).stream().map(i -> i.toString()).collect(Collectors.toList());
          List<String> si2 = ctx.sideInput(chars).stream().collect(Collectors.toList());
          Collections.sort(si);
          Collections.sort(si2);
          final String integers = String.join(",", si);
          final String characters = String.join(",", si2);
          keyGroup
              .getValue()
              .getAll(new TupleTag<String>("lhs"))
              .forEach(
                  lhs ->
                      keyGroup
                          .getValue()
                          .getAll(new TupleTag<String>("rhs"))
                          .forEach(
                              rhs ->
                                  outputConsumer.accept(
                                      lhs + "-" + rhs + "-" + integers + "-" + characters)));
        };

    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            sources,
            targetParallelism,
            null,
            sideMergeFunction,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            Arrays.asList(ints, chars),
            (numBuckets, numShards, hashType) -> TestBucketMetadata.of(numBuckets, numShards),
            new TestFileOperations(),
            ".txt",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX));
    runAndValidate(targetParallelism, expectedNumBuckets, expectedWithSides);
  }

  private void test(TargetParallelism targetParallelism, int expectedNumBuckets) throws Exception {
    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            sources,
            targetParallelism,
            mergeFunction,
            null,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            null,
            (numBuckets, numShards, hashType) -> TestBucketMetadata.of(numBuckets, numShards),
            new TestFileOperations(),
            ".txt",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX));
    runAndValidate(targetParallelism, expectedNumBuckets, expected);
  }

  private void runAndValidate(
      TargetParallelism targetParallelism, int expectedNumBuckets, Set<String> expected)
      throws Exception {
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
            "SortedBucketTransform-KeyGroupSize", DistributionResult.create(9, 7, 1, 2)));
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

    for (BucketShardId bucketShardId : metadata.getAllBucketShardIds()) {
      final FileOperations.Reader<String> outputReader = new TestFileOperations().createReader();
      final ResourceId resourceId =
          fileAssignment.forBucket(
              BucketShardId.of(bucketShardId.getBucketId(), bucketShardId.getShardId()), metadata);
      outputReader.prepareRead(FileSystems.open(resourceId));
      final ArrayList<String> lines = Lists.newArrayList(outputReader.iterator());
      bucketsToOutputs.put(
          BucketShardId.of(bucketShardId.getBucketId(), bucketShardId.getShardId()), lines);
    }

    Assert.assertSame(
        "Found unexpected null-key bucket written in SortedBucketTransform output",
        FileSystems.match(fileAssignment.forNullKeys().toString(), EmptyMatchTreatment.DISALLOW)
            .status(),
        Status.NOT_FOUND);

    return KV.of((TestBucketMetadata) metadata, bucketsToOutputs);
  }
}
