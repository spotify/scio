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
import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.PrimaryKeyedBucketedInput;
import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.PrimaryAndSecondaryKeyedBucktedInput;
import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR;
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
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.Predicate;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptions;
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

  private static final String LHS_FILENAME_PREFIX = "lhs-filename-prefix"; // Custom prefix
  private static final String RHS_FILENAME_PREFIX = "bucket"; // Default prefix
  private static final String FILENAME_SUFFIX = ".txt";

  @Before
  public void setup() {
    lhsPolicy = new SMBFilenamePolicy(fromFolder(lhsFolder), LHS_FILENAME_PREFIX, FILENAME_SUFFIX);
    rhsPolicy = new SMBFilenamePolicy(fromFolder(rhsFolder), RHS_FILENAME_PREFIX, FILENAME_SUFFIX);
  }

  @Test
  public void testBucketedInputMetadata() throws Exception {
    List<String> inputDirectories = new LinkedList<>();

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
      inputDirectories.add(dest.getAbsolutePath());
    }

    // Test with source-compatible input directories
    final BucketedInput<String> validBucketedInput =
        new PrimaryKeyedBucketedInput<>(
            new TupleTag<>("testInput"),
            inputDirectories.subList(0, 9),
            FILENAME_SUFFIX,
            new TestFileOperations(),
            null);

    Assert.assertEquals(validBucketedInput.getSourceMetadata().leastNumBuckets(), 1);
    Assert.assertEquals(
        SourceSpec.from(Collections.singletonList(validBucketedInput)).greatestNumBuckets,
        (int) Math.pow(2.0, 1.0 * 8));

    // Test when metadata aren't same-source compatible
    final BucketedInput<String> invalidBucketedInput =
        new PrimaryKeyedBucketedInput<>(
            new TupleTag<>("testInput"),
            inputDirectories,
            FILENAME_SUFFIX,
            new TestFileOperations(),
            null);
    Assert.assertThrows(IllegalStateException.class, invalidBucketedInput::getSourceMetadata);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsOneShard() throws Exception {
    testPrimary(
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
    testPrimary(
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
    testPrimary(
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
    testPrimary(
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

    testPrimary(
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
    testPrimary(
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
  public void testNullKeysIgnored() throws Exception {
    testPrimary(
        ImmutableMap.of(
            BucketShardId.ofNullKey(), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
        ImmutableMap.of(
            BucketShardId.ofNullKey(), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4", "z3", "z4"),
            BucketShardId.of(1, 0), Lists.newArrayList("c2", "c3")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbk() throws Exception {
    testSingleSourceGbkPrimary(null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbkSecondary() throws Exception {
    testSingleSourceGbkSecondary(null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbkWithPredicate() throws Exception {
    testSingleSourceGbkPrimary((vs, v) -> v.startsWith("x"));
    testSingleSourceGbkPrimary((vs, v) -> v.endsWith("1"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbkWithPredicateEmpty() throws Exception {
    testSingleSourceGbkPrimary((vs, v) -> v.startsWith("z")); // matches no values
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSingleSourceGbkWithSecondaryWithPredicate() throws Exception {
    // FIXME
    throw new IllegalArgumentException();
  }

  Map<BucketShardId, List<String>> singleSourceGbkInput =
      ImmutableMap.of(
          BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
          BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"));

  private void testSingleSourceGbkPrimary(Predicate<String> predicate) throws Exception {
    int numBuckets = maxId(singleSourceGbkInput.keySet(), BucketShardId::getBucketId) + 1;
    int numShards = maxId(singleSourceGbkInput.keySet(), BucketShardId::getShardId) + 1;
    BucketMetadata<String, Void, String> metadata =
        TestBucketMetadata.of(numBuckets, numShards, LHS_FILENAME_PREFIX);
    final TupleTag<String> tag = new TupleTag<>("GBK");
    final TestFileOperations fileOperations = new TestFileOperations();
    final BucketedInput<?> bucketedInput =
        new PrimaryKeyedBucketedInput<>(
            tag,
            Collections.singletonList(lhsFolder.getRoot().getAbsolutePath()),
            FILENAME_SUFFIX,
            fileOperations,
            predicate);
    SortedBucketSource<String> src =
        new SortedBucketPrimaryKeyedSource<>(
            String.class, Collections.singletonList(bucketedInput), null, null);
    checkSingleSourceGbk(metadata, tag, src, metadata::extractKeyPrimary, predicate);
  }

  private void testSingleSourceGbkSecondary(Predicate<String> predicate) throws Exception {
    int numBuckets = maxId(singleSourceGbkInput.keySet(), BucketShardId::getBucketId) + 1;
    int numShards = maxId(singleSourceGbkInput.keySet(), BucketShardId::getShardId) + 1;
    BucketMetadata<String, String, String> metadata =
        TestBucketMetadataWithSecondary.of(numBuckets, numShards, LHS_FILENAME_PREFIX);
    final TupleTag<String> tag = new TupleTag<>("GBK");
    final TestFileOperations fileOperations = new TestFileOperations();
    final BucketedInput<?> bucketedInput =
        new PrimaryAndSecondaryKeyedBucktedInput<>(
            tag,
            Collections.singletonList(lhsFolder.getRoot().getAbsolutePath()),
            FILENAME_SUFFIX,
            fileOperations,
            predicate);
    SortedBucketSource<KV<String, String>> src =
        new SortedBucketPrimaryAndSecondaryKeyedSource<>(
            String.class, String.class, Collections.singletonList(bucketedInput), null, null);
    checkSingleSourceGbk(
        metadata,
        tag,
        src,
        v -> KV.of(metadata.extractKeyPrimary(v), metadata.extractKeySecondary(v)),
        predicate);
  }

  private <KeyType, K2> void checkSingleSourceGbk(
      BucketMetadata<String, K2, String> metadata,
      TupleTag<String> tag,
      SortedBucketSource<KeyType> src,
      Function<String, KeyType> keyFn,
      Predicate<String> predicate)
      throws Exception {
    write(lhsPolicy.forDestination(), metadata, singleSourceGbkInput);
    PCollection<KV<KeyType, CoGbkResult>> output =
        pipeline.apply("SingleSourceGbk-" + UUID.randomUUID(), Read.from(src));
    final Map<KeyType, List<String>> expected =
        filter(groupByKey(singleSourceGbkInput, keyFn), predicate);
    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Map<KeyType, List<String>> actual = new HashMap<>();
              for (Map.Entry<KeyType, CoGbkResult> kv : m.entrySet()) {
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

  static final List<Map<BucketShardId, List<String>>> partitionedInputsMixedBucketsLHS,
      partitionedInputsMixedBucketsRHS,
      partitionedInputsUniformBucketsLHS,
      partitionedInputsUniformBucketsRHS,
      partitionedInputsMixedBucketsAutoParallelismLHS,
      partitionedInputsMixedBucketsAutoParallelismRHS,
      partitionedInputsMixedBucketsMaxParallelismLHS,
      partitionedInputsMixedBucketsMaxParallelismRHS,
      partitionedInputsMixedBucketsCustomParallelismLHS,
      partitionedInputsMixedBucketsCustomParallelismRHS;

  static {
    partitionedInputsMixedBucketsLHS =
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4")));
    partitionedInputsMixedBucketsRHS =
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("x5", "x6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x7", "x8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBuckets() throws Exception {
    testPartitionedPrimary(
        partitionedInputsMixedBucketsLHS,
        partitionedInputsMixedBucketsRHS,
        TargetParallelism.min());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsSecondary() throws Exception {
    testPartitionedSecondary(
        partitionedInputsMixedBucketsLHS,
        partitionedInputsMixedBucketsRHS,
        TargetParallelism.min());
  }

  static {
    partitionedInputsUniformBucketsLHS =
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4"),
                BucketShardId.of(1, 0), Lists.newArrayList("c3", "c4")));
    partitionedInputsUniformBucketsRHS =
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x5", "x6"),
                BucketShardId.of(1, 0), Lists.newArrayList("c5", "c6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("x7", "x8"),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsUniformBuckets() throws Exception {
    testPartitionedPrimary(
        partitionedInputsUniformBucketsLHS,
        partitionedInputsUniformBucketsRHS,
        TargetParallelism.min());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsUniformBucketsSecondary() throws Exception {
    testPartitionedSecondary(
        partitionedInputsUniformBucketsLHS,
        partitionedInputsUniformBucketsRHS,
        TargetParallelism.min());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShardMaxParallelism() throws Exception {
    testPrimary(
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
    testPrimary(
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
    testPrimary(
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

  // For non-minimal parallelism, test input keys *must* hash to their corresponding bucket IDs,
  // since a rehash is required in the merge step

  static {
    Map<BucketShardId, List<String>> partition1Map = new HashMap<>();
    partition1Map.put(BucketShardId.of(0, 0), Lists.newArrayList("w9"));
    partition1Map.put(BucketShardId.of(0, 1), Lists.newArrayList("p1"));
    partition1Map.put(BucketShardId.of(1, 0), Lists.newArrayList("c9"));
    partition1Map.put(BucketShardId.of(1, 1), Lists.newArrayList("c2"));
    partition1Map.put(BucketShardId.of(2, 0), Lists.newArrayList("u1"));
    partition1Map.put(BucketShardId.of(2, 1), Lists.newArrayList("u2"));
    partition1Map.put(BucketShardId.of(3, 0), Lists.newArrayList("b1"));
    partition1Map.put(BucketShardId.of(3, 1), Lists.newArrayList());

    partitionedInputsMixedBucketsAutoParallelismLHS =
        ImmutableList.of(
            partition1Map, ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w3", "w4")));

    partitionedInputsMixedBucketsAutoParallelismRHS =
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w5", "w6")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList(),
                BucketShardId.of(1, 0), Lists.newArrayList("c7", "c8")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsAutoParallelism() throws Exception {
    testPartitionedPrimary(
        partitionedInputsMixedBucketsAutoParallelismLHS,
        partitionedInputsMixedBucketsAutoParallelismRHS,
        TargetParallelism.auto());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsAutoParallelismSecondary() throws Exception {
    testPartitionedSecondary(
        partitionedInputsMixedBucketsAutoParallelismLHS,
        partitionedInputsMixedBucketsAutoParallelismRHS,
        TargetParallelism.auto());
  }

  static {
    partitionedInputsMixedBucketsMaxParallelismLHS =
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("c1", "w1", "x1", "z1")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w1", "w2")));
    partitionedInputsMixedBucketsMaxParallelismRHS =
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w5", "w6"),
                BucketShardId.of(1, 0), Lists.newArrayList("t1", "t2", "x1")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w3", "w4"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2"),
                BucketShardId.of(2, 0), Lists.newArrayList("u1", "u2"),
                BucketShardId.of(3, 0), Lists.newArrayList("r1", "r2")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsMaxParallelism() throws Exception {
    testPartitionedPrimary(
        partitionedInputsMixedBucketsMaxParallelismLHS,
        partitionedInputsMixedBucketsMaxParallelismRHS,
        TargetParallelism.max());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsMaxParallelismSecondary() throws Exception {
    testPartitionedSecondary(
        partitionedInputsMixedBucketsMaxParallelismLHS,
        partitionedInputsMixedBucketsMaxParallelismRHS,
        TargetParallelism.max());
  }

  static {
    partitionedInputsMixedBucketsCustomParallelismLHS =
        ImmutableList.of(
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("c1", "w1", "x1", "z1")),
            ImmutableMap.of(BucketShardId.of(0, 0), Lists.newArrayList("w1", "w2")));
    partitionedInputsMixedBucketsCustomParallelismRHS =
        ImmutableList.of(
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w5", "w6"),
                BucketShardId.of(1, 0), Lists.newArrayList("t1", "t2", "x1")),
            ImmutableMap.of(
                BucketShardId.of(0, 0), Lists.newArrayList("w3", "w4"),
                BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2"),
                BucketShardId.of(2, 0), Lists.newArrayList("u1", "u2"),
                BucketShardId.of(3, 0), Lists.newArrayList("r1", "r2")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionedInputsMixedBucketsCustomParallelism() throws Exception {
    testPartitionedPrimary(
        partitionedInputsMixedBucketsCustomParallelismLHS,
        partitionedInputsMixedBucketsCustomParallelismRHS,
        TargetParallelism.of(2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRecursiveSourceSplit() throws Exception {
    writeSmbSourceWithBytes(800, 8, 1, lhsPolicy);

    final List<BucketedInput<?>> inputs =
        Collections.singletonList(
            new PrimaryKeyedBucketedInput<String>(
                new TupleTag<>("lhs"),
                Collections.singletonList(lhsPolicy.forDestination().getDirectory().toString()),
                FILENAME_SUFFIX,
                new TestFileOperations(),
                null));

    final SortedBucketSource<String> source =
        new SortedBucketPrimaryKeyedSource<>(String.class, inputs, TargetParallelism.auto(), null);

    Assert.assertEquals(800, source.getEstimatedSizeBytes(PipelineOptionsFactory.create()));

    final List<SortedBucketSource<String>> firstSplit = splitAndSort(source, 400);
    // Split into 2 source of size 400 bytes each
    Assert.assertEquals(2, firstSplit.size());
    firstSplit.forEach(s -> Assert.assertEquals(2, s.getEffectiveParallelism()));

    final SortedBucketSource<String> split1 = firstSplit.get(0);
    final SortedBucketSource<String> split2 = firstSplit.get(1);

    Assert.assertEquals(0, split1.getBucketOffset());
    Assert.assertEquals(1, split2.getBucketOffset());

    // Split 1 of the sources again into 4 sources of 100 bytes each
    List<SortedBucketSource<String>> secondSplit = splitAndSort(split1, 100);
    secondSplit.forEach(s -> Assert.assertEquals(8, s.getEffectiveParallelism()));
    Assert.assertEquals(4, secondSplit.size());
    Assert.assertEquals(0, secondSplit.get(0).getBucketOffset());
    Assert.assertEquals(2, secondSplit.get(1).getBucketOffset());
    Assert.assertEquals(4, secondSplit.get(2).getBucketOffset());
    Assert.assertEquals(6, secondSplit.get(3).getBucketOffset());

    // Split the other source again into 2 sources of 200 bytes each
    secondSplit = splitAndSort(split2, 200);
    Assert.assertEquals(2, secondSplit.size());
    Assert.assertEquals(1, secondSplit.get(0).getBucketOffset());
    Assert.assertEquals(3, secondSplit.get(1).getBucketOffset());
    secondSplit.forEach(s -> Assert.assertEquals(4, s.getEffectiveParallelism()));
  }

  @SuppressWarnings("unchecked")
  private static List<SortedBucketSource<String>> splitAndSort(
      SortedBucketSource<String> source, int desiredByteSize) throws Exception {
    final PipelineOptions opts = PipelineOptionsFactory.create();
    final List<SortedBucketSource<String>> splitSources =
        (List<SortedBucketSource<String>>)
            source.split((long) (desiredByteSize / DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR), opts);
    splitSources.sort(Comparator.comparingInt(SortedBucketSource::getBucketOffset));

    return splitSources;
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

  private void testPrimary(
      Map<BucketShardId, List<String>> lhsInput, Map<BucketShardId, List<String>> rhsInput)
      throws Exception {
    testPrimary(lhsInput, rhsInput, null, null, TargetParallelism.min());
  }

  private void testPrimary(
      Map<BucketShardId, List<String>> lhsInput,
      Map<BucketShardId, List<String>> rhsInput,
      TargetParallelism targetParallelism)
      throws Exception {
    testPrimary(lhsInput, rhsInput, null, null, targetParallelism);
  }

  class TestInput<KeyType, K2> {
    TupleTag<String> tag;
    Map<BucketShardId, List<String>> input;
    BucketMetadata<String, K2, String> metadata;
    BucketedInput<?> bucketedInput;
    Map<KeyType, List<String>> expected;
    Function<String, KeyType> keyFn;

    public TestInput(
        TupleTag<String> tag,
        Map<BucketShardId, List<String>> input,
        BucketMetadata<String, K2, String> metadata,
        Function<String, KeyType> keyFn,
        BucketedInput<?> bucketedInput,
        Map<KeyType, List<String>> expected) {
      this.tag = tag;
      this.input = input;
      this.metadata = metadata;
      this.bucketedInput = bucketedInput;
      this.expected = expected;
      this.keyFn = keyFn;
    }
  }

  private TestInput<String, Void> testInputPrimary(
      String tagName,
      Map<BucketShardId, List<String>> input,
      Predicate<String> predicate,
      TestFileOperations fileOperations,
      String prefix,
      List<String> paths)
      throws Exception {
    int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
    int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;
    final TupleTag<String> tag = new TupleTag<>(tagName);
    TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards, prefix);
    Function<String, String> keyFn = metadata::extractKeyPrimary;
    return new TestInput<>(
        tag,
        input,
        metadata,
        keyFn,
        new PrimaryKeyedBucketedInput<>(tag, paths, FILENAME_SUFFIX, fileOperations, predicate),
        filter(groupByKey(input, keyFn), predicate));
  }

  private TestInput<KV<String, String>, String> testInputSecondary(
      String tagName,
      Map<BucketShardId, List<String>> input,
      Predicate<String> predicate,
      TestFileOperations fileOperations,
      String prefix,
      List<String> paths)
      throws Exception {
    int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
    int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;
    final TupleTag<String> tag = new TupleTag<>(tagName);
    TestBucketMetadataWithSecondary metadata =
        TestBucketMetadataWithSecondary.of(numBuckets, numShards, prefix);
    Function<String, KV<String, String>> keyFn =
        v -> KV.of(metadata.extractKeyPrimary(v), metadata.extractKeySecondary(v));
    return new TestInput<>(
        tag,
        input,
        metadata,
        keyFn,
        new PrimaryAndSecondaryKeyedBucktedInput<>(
            tag, paths, FILENAME_SUFFIX, fileOperations, predicate),
        filter(groupByKey(input, keyFn), predicate));
  }

  private void testPrimary(
      Map<BucketShardId, List<String>> lhsInput,
      Map<BucketShardId, List<String>> rhsInput,
      Predicate<String> lhsPredicate,
      Predicate<String> rhsPredicate,
      TargetParallelism targetParallelism)
      throws Exception {
    final TestFileOperations fileOperations = new TestFileOperations();
    TestInput<String, Void> lhs =
        testInputPrimary(
            "LHS",
            lhsInput,
            lhsPredicate,
            fileOperations,
            LHS_FILENAME_PREFIX,
            Collections.singletonList(lhsFolder.getRoot().getAbsolutePath()));
    TestInput<String, Void> rhs =
        testInputPrimary(
            "RHS",
            rhsInput,
            rhsPredicate,
            fileOperations,
            RHS_FILENAME_PREFIX,
            Collections.singletonList(rhsFolder.getRoot().getAbsolutePath()));

    test(
        lhs,
        rhs,
        new SortedBucketPrimaryKeyedSource<>(
            String.class,
            Lists.newArrayList(lhs.bucketedInput, rhs.bucketedInput),
            targetParallelism,
            null));
  }

  private void testSecondary(
      Map<BucketShardId, List<String>> lhsInput,
      Map<BucketShardId, List<String>> rhsInput,
      Predicate<String> lhsPredicate,
      Predicate<String> rhsPredicate,
      TargetParallelism targetParallelism)
      throws Exception {
    final TestFileOperations fileOperations = new TestFileOperations();
    TestInput<KV<String, String>, String> lhs =
        testInputSecondary(
            "LHS",
            lhsInput,
            lhsPredicate,
            fileOperations,
            LHS_FILENAME_PREFIX,
            Collections.singletonList(lhsFolder.getRoot().getAbsolutePath()));
    TestInput<KV<String, String>, String> rhs =
        testInputSecondary(
            "RHS",
            rhsInput,
            rhsPredicate,
            fileOperations,
            RHS_FILENAME_PREFIX,
            Collections.singletonList(rhsFolder.getRoot().getAbsolutePath()));

    test(
        lhs,
        rhs,
        new SortedBucketPrimaryAndSecondaryKeyedSource<>(
            String.class,
            String.class,
            Lists.newArrayList(lhs.bucketedInput, rhs.bucketedInput),
            targetParallelism,
            null));
  }

  private <KeyType, K2> void test(
      TestInput<KeyType, K2> lhs, TestInput<KeyType, K2> rhs, SortedBucketSource<KeyType> src)
      throws Exception {
    write(lhsPolicy.forDestination(), lhs.metadata, lhs.input);
    write(rhsPolicy.forDestination(), rhs.metadata, rhs.input);
    checkJoin(pipeline, lhs.tag, rhs.tag, lhs.expected, rhs.expected, src);

    final PipelineResult result = pipeline.run();
    // Verify Metrics
    Function<String, KeyType> keyFn = lhs.keyFn;
    final Map<KeyType, Integer> keyGroupCounts =
        Stream.concat(lhs.input.values().stream(), rhs.input.values().stream())
            .flatMap(List::stream)
            .filter(element -> !element.equals("")) // filter out null keys
            .collect(Collectors.toMap(keyFn, str -> 1, Integer::sum));

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

  private Map<BucketShardId, List<String>> mergePartitions(
      List<Map<BucketShardId, List<String>>> inputs) {
    Map<BucketShardId, List<String>> allValues = new HashMap<>();
    inputs.forEach(
        input ->
            input.forEach(
                (k, v) ->
                    allValues.merge(
                        k,
                        v,
                        (v1, v2) -> {
                          List<String> newList = new LinkedList<>(v1);
                          newList.addAll(v2);
                          return newList;
                        })));
    return allValues;
  }

  private <K2> List<ResourceId> writePartitionDests(
      List<Map<BucketShardId, List<String>>> inputs,
      String pathPrefix,
      String filenamePrefix,
      BiFunction<Integer, Integer, BucketMetadata<String, K2, String>> mdFn)
      throws Exception {
    List<ResourceId> dests = new ArrayList<>();
    for (Map<BucketShardId, List<String>> input : inputs) {
      int numBuckets = maxId(input.keySet(), BucketShardId::getBucketId) + 1;
      int numShards = maxId(input.keySet(), BucketShardId::getShardId) + 1;
      BucketMetadata<String, K2, String> metadata = mdFn.apply(numBuckets, numShards);
      ResourceId destination =
          LocalResources.fromFile(
              partitionedInputFolder.newFolder(pathPrefix + inputs.indexOf(input)), true);
      FileAssignment fileAssignment =
          new SMBFilenamePolicy(destination, filenamePrefix, FILENAME_SUFFIX).forDestination();
      write(fileAssignment, metadata, input);
      dests.add(destination);
    }
    return dests;
  }

  private void testPartitionedPrimary(
      List<Map<BucketShardId, List<String>>> lhsInputs,
      List<Map<BucketShardId, List<String>>> rhsInputs,
      TargetParallelism targetParallelism)
      throws Exception {
    final TestFileOperations fileOperations = new TestFileOperations();

    Map<BucketShardId, List<String>> allLhsValues = mergePartitions(lhsInputs);
    List<ResourceId> lhsDests =
        writePartitionDests(
            lhsInputs,
            "lhs",
            LHS_FILENAME_PREFIX,
            (b, s) -> TestBucketMetadata.of(b, s, LHS_FILENAME_PREFIX));
    TestInput<String, Void> lhs =
        testInputPrimary(
            "LHS",
            allLhsValues,
            null,
            fileOperations,
            LHS_FILENAME_PREFIX,
            lhsDests.stream().map(ResourceId::toString).collect(Collectors.toList()));

    Map<BucketShardId, List<String>> allRhsValues = mergePartitions(rhsInputs);
    List<ResourceId> rhsDests =
        writePartitionDests(
            rhsInputs,
            "rhs",
            RHS_FILENAME_PREFIX,
            (b, s) -> TestBucketMetadata.of(b, s, RHS_FILENAME_PREFIX));
    TestInput<String, Void> rhs =
        testInputPrimary(
            "RHS",
            allRhsValues,
            null,
            fileOperations,
            RHS_FILENAME_PREFIX,
            rhsDests.stream().map(ResourceId::toString).collect(Collectors.toList()));

    SortedBucketSource<String> src =
        new SortedBucketPrimaryKeyedSource<>(
            String.class,
            Lists.newArrayList(lhs.bucketedInput, rhs.bucketedInput),
            targetParallelism,
            null);

    checkJoin(pipeline, lhs.tag, rhs.tag, lhs.expected, rhs.expected, src);
    pipeline.run();
  }

  private void testPartitionedSecondary(
      List<Map<BucketShardId, List<String>>> lhsInputs,
      List<Map<BucketShardId, List<String>>> rhsInputs,
      TargetParallelism targetParallelism)
      throws Exception {
    final TestFileOperations fileOperations = new TestFileOperations();

    Map<BucketShardId, List<String>> allLhsValues = mergePartitions(lhsInputs);
    List<ResourceId> lhsDests =
        writePartitionDests(
            lhsInputs,
            "lhs",
            LHS_FILENAME_PREFIX,
            (b, s) -> TestBucketMetadataWithSecondary.of(b, s, LHS_FILENAME_PREFIX));
    TestInput<KV<String, String>, String> lhs =
        testInputSecondary(
            "LHS",
            allLhsValues,
            null,
            fileOperations,
            LHS_FILENAME_PREFIX,
            lhsDests.stream().map(ResourceId::toString).collect(Collectors.toList()));

    Map<BucketShardId, List<String>> allRhsValues = mergePartitions(rhsInputs);
    List<ResourceId> rhsDests =
        writePartitionDests(
            rhsInputs,
            "rhs",
            RHS_FILENAME_PREFIX,
            (b, s) -> TestBucketMetadataWithSecondary.of(b, s, RHS_FILENAME_PREFIX));
    TestInput<KV<String, String>, String> rhs =
        testInputSecondary(
            "RHS",
            allRhsValues,
            null,
            fileOperations,
            RHS_FILENAME_PREFIX,
            rhsDests.stream().map(ResourceId::toString).collect(Collectors.toList()));

    SortedBucketSource<KV<String, String>> src =
        new SortedBucketPrimaryAndSecondaryKeyedSource<>(
            String.class,
            String.class,
            Lists.newArrayList(lhs.bucketedInput, rhs.bucketedInput),
            targetParallelism,
            null);

    checkJoin(pipeline, lhs.tag, rhs.tag, lhs.expected, rhs.expected, src);
    pipeline.run();
  }

  private static <KeyType> void checkJoin(
      TestPipeline pipeline,
      TupleTag<String> lhsTag,
      TupleTag<String> rhsTag,
      Map<KeyType, List<String>> lhsExpected,
      Map<KeyType, List<String>> rhsExpected,
      SortedBucketSource<KeyType> src)
      throws Exception {
    PCollection<KV<KeyType, CoGbkResult>> output =
        pipeline.apply("CheckJoin-" + UUID.randomUUID(), Read.from(src));
    // CoGroupByKey inputs as expected result
    final Map<KeyType, KV<List<String>, List<String>>> expected = new HashMap<>();
    for (KeyType k : Sets.union(lhsExpected.keySet(), rhsExpected.keySet())) {
      List<String> l = lhsExpected.getOrDefault(k, Collections.emptyList());
      List<String> r = rhsExpected.getOrDefault(k, Collections.emptyList());
      expected.put(k, KV.of(l, r));
    }

    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Map<KeyType, KV<List<String>, List<String>>> actual = new HashMap<>();
              for (Map.Entry<KeyType, CoGbkResult> kv : m.entrySet()) {
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

  private static <K2> void write(
      FileAssignment fileAssignment,
      BucketMetadata<String, K2, String> metadata,
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

  private static <KeyType> Map<KeyType, List<String>> groupByKey(
      Map<BucketShardId, List<String>> input, Function<String, KeyType> keyFn) {
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

  private static <KeyType> Map<KeyType, List<String>> filter(
      Map<KeyType, List<String>> input, Predicate<String> predicate) {
    if (predicate == null) {
      return input;
    } else {
      Map<KeyType, List<String>> filtered = new HashMap<>();
      for (Map.Entry<KeyType, List<String>> e : input.entrySet()) {
        List<String> value = new ArrayList<>();
        e.getValue()
            .forEach(
                v -> {
                  if (predicate.apply(value, v)) {
                    value.add(v);
                  }
                });
        filtered.put(e.getKey(), value);
      }
      // if predicate removes all values, remove key group
      List<KeyType> toRemove =
          filtered.keySet().stream()
              .filter(k -> filtered.get(k).isEmpty())
              .collect(Collectors.toList());
      toRemove.forEach(filtered::remove);
      return filtered;
    }
  }

  static void verifyMetrics(
      PipelineResult result, Map<String, DistributionResult> expectedDistributions) {
    final Map<String, DistributionResult> actualDistributions =
        ImmutableList.copyOf(result.metrics().allMetrics().getDistributions().iterator()).stream()
            .collect(
                Collectors.toMap(
                    metric -> metric.getName().getName().replaceAll("\\{\\d+}", ""),
                    MetricResult::getCommitted));

    Assert.assertEquals(expectedDistributions, actualDistributions);
  }
}
