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

import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link SortedBucketSource}. */
public class SortedBucketSourceTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder lhsFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder rhsFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder partitionedInputFolder = new TemporaryFolder();

  private SMBFilenamePolicy lhsPolicy;
  private SMBFilenamePolicy rhsPolicy;

  @Before
  public void setup() {
    lhsPolicy = new SMBFilenamePolicy(fromFolder(lhsFolder), ".txt");
    rhsPolicy = new SMBFilenamePolicy(fromFolder(rhsFolder), ".txt");
  }

  @Test
  public void testBucketedInputMetadata() throws Exception {
    List<ResourceId> inputDirectories = new LinkedList<>();

    // first 9 elements are source-compatible, last is not
    for (int i = 0; i < 10; i++) {
      final TestBucketMetadata metadata =
          TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1).withKeyIndex(i < 9 ? 0 : 1);
      final File dest = lhsFolder.newFolder(String.valueOf(i));

      final OutputStream outputStream =
          Channels.newOutputStream(
              FileSystems.create(
                  LocalResources.fromFile(lhsFolder.newFile(i + "/metadata.json"), false),
                  "application/json"));

      BucketMetadata.to(metadata, outputStream);
      inputDirectories.add(LocalResources.fromFile(dest, true));
    }

    // Test with source-compatible input directories
    final BucketedInput validBucketedInput =
        new BucketedInput<>(
            new TupleTag<>("testInput"),
            inputDirectories.subList(0, 8),
            ".txt",
            new TestFileOperations());

    // Canonical metadata should have the smallest bucket count
    Assert.assertEquals(validBucketedInput.getMetadata().getNumBuckets(), 1);

    // Test when metadata aren't same-source compatible
    final BucketedInput invalidBucketedInput =
        new BucketedInput<>(
            new TupleTag<>("testInput"), inputDirectories, ".txt", new TestFileOperations());

    Assert.assertThrows(IllegalStateException.class, invalidBucketedInput::getMetadata);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsOneShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "a4", "c3", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "x4", "z3", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsMultiShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "b1"),
            BucketShardId.of(0, 1), Lists.newArrayList("a2", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "y1"),
            BucketShardId.of(1, 1), Lists.newArrayList("x2", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsMixedShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsOneShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"),
            BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMultiShard() throws Exception {
    Map<BucketShardId, List<String>> lhs = new HashMap<>();
    lhs.put(BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"));
    lhs.put(BucketShardId.of(0, 1), Lists.newArrayList("a1", "a2", "b1", "b2"));
    lhs.put(BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"));
    lhs.put(BucketShardId.of(1, 1), Lists.newArrayList("x1", "x2", "y1", "y2"));
    lhs.put(BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"));
    lhs.put(BucketShardId.of(2, 1), Lists.newArrayList("c1", "c2", "d1", "d2"));
    lhs.put(BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2"));
    lhs.put(BucketShardId.of(3, 1), Lists.newArrayList("y1", "y2", "z1", "z2"));

    test(
        lhs,
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"),
            BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("f4", "g4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNullKeysTestd() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.ofNullKey(0), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
        ImmutableMap.of(
            BucketShardId.ofNullKey(0), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4", "z3", "z4"),
            BucketShardId.of(1, 0), Lists.newArrayList("c2", "c3")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbk() throws Exception {
    Map<BucketShardId, List<String>> input =
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"));

    int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
    int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;

    TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);

    write(lhsPolicy.forDestination(), metadata, input);

    final TupleTag<String> tag = new TupleTag<>("GBK");
    final TestFileOperations fileOperations = new TestFileOperations();
    final BucketedInput<?, ?> bucketedInput =
        new BucketedInput<>(tag, fromFolder(lhsFolder), ".txt", fileOperations);

    PCollection<KV<String, CoGbkResult>> output =
        pipeline.apply(
            Read.from(
                new SortedBucketSource<>(String.class, Collections.singletonList(bucketedInput))));

    final Map<String, List<String>> expected = groupByKey(input, metadata::extractKey);

    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Map<String, List<String>> actual = new HashMap<>();
              for (Map.Entry<String, CoGbkResult> kv : m.entrySet()) {
                List<String> v =
                    StreamSupport.stream(kv.getValue().getAll(tag).spliterator(), false)
                        .sorted()
                        .collect(Collectors.toList());
                actual.put(kv.getKey(), v);
              }
              Assert.assertEquals(expected, actual);
              return null;
            });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBuckets() throws Exception {
    testPartitioned(
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4"))),
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("x5", "x6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x7", "x8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8"))));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsUniformBuckets() throws Exception {
    testPartitioned(
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4"),
                BucketShardId.of(1, 0), Lists.newArrayList("c3", "c4"))),
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x5", "x6"),
                BucketShardId.of(1, 0), Lists.newArrayList("c5", "c6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x7", "x8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8"))));
  }

  // For non-minimum parallelism, test input keys *must* hash to their corresponding bucket IDs,
  // since a rehash is required in the merge step
  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsMaxParallelism() throws Exception {
    testPartitioned(
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w1", "w2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w3", "w4"))),
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w5", "w6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w7", "w8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8"))),
        TargetParallelism.max());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShardMaxParallelism() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e1", "e2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2", "c3"),
            BucketShardId.of(2, 0), Lists.newArrayList("i1", "i2", "i1", "i2"),
            BucketShardId.of(3, 0), Lists.newArrayList("k1", "k2", "k1", "k2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e3", "e3"),
            BucketShardId.of(0, 1), Lists.newArrayList("m4", "m4"),
            BucketShardId.of(1, 0), Lists.newArrayList("t3", "t3"),
            BucketShardId.of(1, 1), Lists.newArrayList("h4", "h4")),
        TargetParallelism.max());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShardCustomParallelism() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e1", "e2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2", "c3"),
            BucketShardId.of(2, 0), Lists.newArrayList("i1", "i2", "i1", "i2"),
            BucketShardId.of(3, 0), Lists.newArrayList("k1", "k2", "k1", "k2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e3", "e3"),
            BucketShardId.of(0, 1), Lists.newArrayList("m4", "m4")),
        TargetParallelism.of(2));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShardAutoParallelism() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e1", "e2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2", "c3"),
            BucketShardId.of(2, 0), Lists.newArrayList("i1", "i2", "i1", "i2"),
            BucketShardId.of(3, 0), Lists.newArrayList("k1", "k2", "k1", "k2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("e3", "e3"),
            BucketShardId.of(0, 1), Lists.newArrayList("m4", "m4")),
        TargetParallelism.auto());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsAutoParallelism() throws Exception {
    testPartitioned(
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w1", "w2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w3", "w4"))),
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w5", "w6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w7", "w8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8"))),
        TargetParallelism.auto());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSourceSplit() throws Exception {
    writeSmbSourceWithBytes(80, 4, 1, lhsPolicy);
    writeSmbSourceWithBytes(60, 1, 2, rhsPolicy);

    final List<BucketedInput<?, ?>> inputs =
        Lists.newArrayList(
            new BucketedInput<String, String>(
                new TupleTag<>("lhs"),
                lhsPolicy.forDestination().getDirectory(),
                ".txt",
                new TestFileOperations()),
            new BucketedInput<>(
                new TupleTag<>("rhs"),
                rhsPolicy.forDestination().getDirectory(),
                ".txt",
                new TestFileOperations()));

    final SortedBucketSource source =
        new SortedBucketSource(String.class, inputs, TargetParallelism.auto());

    final List<SortedBucketSource<String>> splitSources1 =
        source.split(150, PipelineOptionsFactory.create());
    splitSources1.sort(Comparator.comparingInt(SortedBucketSource::getBucketOffset));

    Assert.assertEquals(2, splitSources1.size());
    Assert.assertEquals(0, splitSources1.get(0).getBucketOffset());
    Assert.assertEquals(1, splitSources1.get(1).getBucketOffset());

    final List<SortedBucketSource<String>> splitSources2 =
        source.split(90, PipelineOptionsFactory.create());
    splitSources2.sort(Comparator.comparingInt(SortedBucketSource::getBucketOffset));

    Assert.assertEquals(4, splitSources2.size());
    Assert.assertEquals(0, splitSources2.get(0).getBucketOffset());
    Assert.assertEquals(1, splitSources2.get(1).getBucketOffset());
    Assert.assertEquals(2, splitSources2.get(2).getBucketOffset());
    Assert.assertEquals(3, splitSources2.get(3).getBucketOffset());
  }

  private void writeSmbSourceWithBytes(
      int desiredByteSize, int numBuckets, int numShards, SMBFilenamePolicy filenamePolicy)
      throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);

    // Create string input with desired size in bytes
    final List<String> inputPerBucketShard =
        IntStream.range(0, desiredByteSize / (numBuckets * numShards * 2))
            .boxed()
            .map(i -> "x")
            .collect(Collectors.toList());

    final Map<BucketShardId, List<String>> inputMap = new HashMap<>();
    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      for (int shardId = 0; shardId < numShards; shardId++) {
        inputMap.put(BucketShardId.of(bucketId, shardId), inputPerBucketShard);
      }
    }

    write(filenamePolicy.forDestination(), metadata, inputMap);
  }

  private void test(
      Map<BucketShardId, List<String>> lhsInput, Map<BucketShardId, List<String>> rhsInput)
      throws Exception {
    test(lhsInput, rhsInput, TargetParallelism.min());
  }

  private void test(
      Map<BucketShardId, List<String>> lhsInput,
      Map<BucketShardId, List<String>> rhsInput,
      TargetParallelism targetParallelism)
      throws Exception {
    int lhsNumBuckets = maxId(lhsInput.keySet(), BucketShardId::getBucketId) + 1;
    int lhsNumShards = maxId(lhsInput.keySet(), BucketShardId::getShardId) + 1;

    int rhsNumBuckets = maxId(rhsInput.keySet(), BucketShardId::getBucketId) + 1;
    int rhsNumShards = maxId(rhsInput.keySet(), BucketShardId::getShardId) + 1;

    TestBucketMetadata lhsMetadata = TestBucketMetadata.of(lhsNumBuckets, lhsNumShards);
    TestBucketMetadata rhsMetadata = TestBucketMetadata.of(rhsNumBuckets, rhsNumShards);

    write(lhsPolicy.forDestination(), lhsMetadata, lhsInput);
    write(rhsPolicy.forDestination(), rhsMetadata, rhsInput);

    checkJoin(
        pipeline,
        Collections.singletonList(fromFolder(lhsFolder)),
        Collections.singletonList(fromFolder(rhsFolder)),
        lhsInput,
        rhsInput,
        targetParallelism);

    final PipelineResult result = pipeline.run();

    // Verify Metrics
    final Map<String, Integer> keyGroupCounts =
        Stream.concat(lhsInput.values().stream(), rhsInput.values().stream())
            .flatMap(List::stream)
            .filter(element -> !element.equals("")) // filter out null keys
            .collect(Collectors.toMap(lhsMetadata::extractKey, str -> 1, Integer::sum));

    final long elementsRead = keyGroupCounts.values().stream().reduce(0, Integer::sum);

    verifyMetrics(
        result,
        ImmutableMap.of(
            "SortedBucketSource-KeyGroupSize",
            DistributionResult.create(
                elementsRead,
                keyGroupCounts.keySet().size(),
                keyGroupCounts.values().stream().min(Integer::compareTo).get(),
                keyGroupCounts.values().stream().max(Integer::compareTo).get())));
  }

  private void testPartitioned(
      List<Map<BucketShardId, List<String>>> lhsInputs,
      List<Map<BucketShardId, List<String>>> rhsInputs)
      throws Exception {
    testPartitioned(lhsInputs, rhsInputs, TargetParallelism.min());
  }

  private void testPartitioned(
      List<Map<BucketShardId, List<String>>> lhsInputs,
      List<Map<BucketShardId, List<String>>> rhsInputs,
      TargetParallelism targetParallelism)
      throws Exception {

    List<ResourceId> lhsPaths = new ArrayList<>();
    Map<BucketShardId, List<String>> allLhsValues = new HashMap<>();

    for (Map<BucketShardId, List<String>> input : lhsInputs) {
      int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
      int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;
      TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);
      ResourceId destination =
          LocalResources.fromFile(
              partitionedInputFolder.newFolder("lhs" + lhsInputs.indexOf(input)), true);
      FileAssignment fileAssignment = new SMBFilenamePolicy(destination, ".txt").forDestination();
      write(fileAssignment, metadata, input);
      lhsPaths.add(destination);
      input.forEach(
          (k, v) ->
              allLhsValues.merge(
                  k,
                  v,
                  (v1, v2) -> {
                    List<String> newList = new LinkedList<>(v1);
                    newList.addAll(v2);
                    return newList;
                  }));
    }

    List<ResourceId> rhsPaths = new ArrayList<>();
    Map<BucketShardId, List<String>> allRhsValues = new HashMap<>();
    for (Map<BucketShardId, List<String>> input : rhsInputs) {
      int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
      int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;
      TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);
      ResourceId destination =
          LocalResources.fromFile(
              partitionedInputFolder.newFolder("rhs" + rhsInputs.indexOf(input)), true);
      FileAssignment fileAssignment = new SMBFilenamePolicy(destination, ".txt").forDestination();
      write(fileAssignment, metadata, input);
      rhsPaths.add(destination);
      input.forEach(
          (k, v) ->
              allRhsValues.merge(
                  k,
                  v,
                  (v1, v2) -> {
                    List<String> newList = new LinkedList<>(v1);
                    newList.addAll(v2);
                    return newList;
                  }));
    }

    checkJoin(pipeline, lhsPaths, rhsPaths, allLhsValues, allRhsValues, targetParallelism);
    pipeline.run();
  }

  private static void checkJoin(
      TestPipeline pipeline,
      List<ResourceId> lhsPaths,
      List<ResourceId> rhsPaths,
      Map<BucketShardId, List<String>> lhsValues,
      Map<BucketShardId, List<String>> rhsValues,
      TargetParallelism targetParallelism)
      throws Exception {
    final TupleTag<String> lhsTag = new TupleTag<>("LHS");
    final TupleTag<String> rhsTag = new TupleTag<>("RHS");
    final TestFileOperations fileOperations = new TestFileOperations();
    final List<BucketedInput<?, ?>> inputs =
        Lists.newArrayList(
            new BucketedInput<>(lhsTag, lhsPaths, ".txt", fileOperations),
            new BucketedInput<>(rhsTag, rhsPaths, ".txt", fileOperations));

    PCollection<KV<String, CoGbkResult>> output =
        pipeline.apply(
            Read.from(new SortedBucketSource<>(String.class, inputs, targetParallelism)));

    Function<String, String> extractKeyFn = TestBucketMetadata.of(2, 1)::extractKey;

    // CoGroupByKey inputs as expected result
    final Map<String, List<String>> lhs = groupByKey(lhsValues, extractKeyFn);
    final Map<String, List<String>> rhs = groupByKey(rhsValues, extractKeyFn);

    final Map<String, KV<List<String>, List<String>>> expected = new HashMap<>();
    for (String k : Sets.union(lhs.keySet(), rhs.keySet())) {
      List<String> l = lhs.getOrDefault(k, Collections.emptyList());
      List<String> r = rhs.getOrDefault(k, Collections.emptyList());
      expected.put(k, KV.of(l, r));
    }

    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Map<String, KV<List<String>, List<String>>> actual = new HashMap<>();
              for (Map.Entry<String, CoGbkResult> kv : m.entrySet()) {
                List<String> l =
                    StreamSupport.stream(kv.getValue().getAll(lhsTag).spliterator(), false)
                        .sorted()
                        .collect(Collectors.toList());
                List<String> r =
                    StreamSupport.stream(kv.getValue().getAll(rhsTag).spliterator(), false)
                        .sorted()
                        .collect(Collectors.toList());
                actual.put(kv.getKey(), KV.of(l, r));
              }
              Assert.assertEquals(expected, actual);
              return null;
            });
  }

  private static void write(
      FileAssignment fileAssignment,
      TestBucketMetadata metadata,
      Map<BucketShardId, List<String>> input)
      throws Exception {
    // Write bucket metadata
    BucketMetadata.to(
        metadata,
        Channels.newOutputStream(
            FileSystems.create(fileAssignment.forMetadata(), "application/json")));

    // Write bucket files
    final TestFileOperations fileOperations = new TestFileOperations();
    for (Map.Entry<BucketShardId, List<String>> entry : input.entrySet()) {
      Writer<String> writer =
          fileOperations.createWriter(fileAssignment.forBucket(entry.getKey(), metadata));
      for (String s : entry.getValue()) {
        writer.write(s);
      }
      writer.close();
    }
  }

  private static int maxId(Set<BucketShardId> ids, ToIntFunction<BucketShardId> fn) {
    return ids.stream().mapToInt(fn).max().getAsInt();
  }

  private static Map<String, List<String>> groupByKey(
      Map<BucketShardId, List<String>> input, Function<String, String> keyFn) {
    final List<String> values =
        input.values().stream().flatMap(List::stream).collect(Collectors.toList());
    return values.stream()
        .filter(v -> keyFn.apply(v) != null)
        .collect(
            Collectors.toMap(
                keyFn,
                Collections::singletonList,
                (l, r) ->
                    Stream.concat(l.stream(), r.stream()).sorted().collect(Collectors.toList())));
  }

  private static void verifyMetrics(
      PipelineResult result, Map<String, DistributionResult> expectedDistributions) {
    final Map<String, DistributionResult> actualDistributions =
        ImmutableList.copyOf(result.metrics().allMetrics().getDistributions().iterator()).stream()
            .collect(
                Collectors.toMap(metric -> metric.getName().getName(), MetricResult::getCommitted));

    Assert.assertEquals(expectedDistributions, actualDistributions);
  }
}
