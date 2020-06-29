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

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.SortedBucketPreKeyedSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link SortedBucketSink}. */
public class SortedBucketSinkTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder output = new TemporaryFolder();
  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  // test input, [a01, a02, ..., a09, a10, b01, ...]
  private static final String[] input =
      Stream.concat(
              IntStream.rangeClosed('a', 'z').boxed(), IntStream.rangeClosed('A', 'Z').boxed())
          .flatMap(
              i ->
                  IntStream.rangeClosed(1, 10)
                      .boxed()
                      .map(j -> String.format("%s%02d", String.valueOf((char) i.intValue()), j)))
          .toArray(String[]::new);

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketOneShard() throws Exception {
    test(1, 1, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketOneShardWithKeyCache() throws Exception {
    test(1, 1, true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketOneShard() throws Exception {
    test(2, 1, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketMultiShard() throws Exception {
    test(1, 2, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketMultiShard() throws Exception {
    test(2, 2, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketOneShardKeyedPCollectionWithKeyCache() throws Exception {
    testKeyedCollection(1, 1, true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketMultiShardKeyedPCollection() throws Exception {
    testKeyedCollection(2, 2, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWritesEmptyBucketFiles() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(2, 2);

    final ResourceId outputDirectory = fromFolder(output);
    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata, outputDirectory, fromFolder(temp), ".txt", new TestFileOperations(), 1);

    pipeline.apply(Create.empty(StringUtf8Coder.of())).apply(sink);
    pipeline.run().waitUntilFinish();

    final FileAssignment dstFiles =
        new SMBFilenamePolicy.FileAssignment(outputDirectory, ".txt", false);

    for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
      for (int shardId = 0; shardId < metadata.getNumShards(); shardId++) {
        Assert.assertSame(
            FileSystems.match(
                    dstFiles.forBucket(BucketShardId.of(bucketId, shardId), metadata).toString(),
                    EmptyMatchTreatment.DISALLOW)
                .status(),
            Status.OK);
      }
    }
    Assert.assertSame(
        FileSystems.match(dstFiles.forMetadata().toString(), EmptyMatchTreatment.DISALLOW).status(),
        Status.OK);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWritesNoFilesIfPriorStepsFail() throws Exception {
    // An exception will be thrown during the temp file write transform
    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            TestBucketMetadata.of(1, 1),
            fromFolder(output),
            fromFolder(temp),
            ".txt",
            new ExceptionThrowingFileOperations(),
            1);

    pipeline.apply(Create.of(Stream.of(input).collect(Collectors.toList()))).apply(sink);

    try {
      pipeline.run();
    } catch (PipelineExecutionException ignored) {
    }

    Assert.assertEquals(0, temp.getRoot().listFiles().length);
    Assert.assertEquals(0, output.getRoot().listFiles().length);
  }

  private void test(int numBuckets, int numShards, boolean useKeyCache) throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);
    final int keyCacheSize = useKeyCache ? 100 : 0;
    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata,
            fromFolder(output),
            fromFolder(temp),
            ".txt",
            new TestFileOperations(),
            1,
            keyCacheSize);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<String> reshuffle = Reshuffle.viaRandomKey();

    check(
        pipeline
            .apply(Create.of(Stream.of(input).collect(Collectors.toList())))
            .apply(reshuffle)
            .apply(sink),
        metadata,
        assertValidSmbFormat(metadata));

    pipeline.run();
  }

  private void testKeyedCollection(int numBuckets, int numShards, boolean useKeyCache)
      throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);
    final int keyCacheSize = useKeyCache ? 100 : 0;

    final SortedBucketPreKeyedSink<String, String> sink =
        new SortedBucketPreKeyedSink<>(
            metadata,
            fromFolder(output),
            fromFolder(temp),
            ".txt",
            new TestFileOperations(),
            1,
            StringUtf8Coder.of(),
            true,
            keyCacheSize);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<KV<String, String>> reshuffle = Reshuffle.viaRandomKey();

    check(
        pipeline
            .apply(
                Create.of(
                    Stream.of(input)
                        .map(s -> KV.of(metadata.extractKey(s), s))
                        .collect(Collectors.toList())))
            .apply(reshuffle)
            .apply(sink),
        metadata,
        assertValidSmbFormat(metadata));

    pipeline.run();
  }

  private static void check(
      WriteResult writeResult,
      TestBucketMetadata metadata,
      Consumer<Map<BucketShardId, List<String>>> checkFn) {
    @SuppressWarnings("unchecked")
    final PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("WrittenMetadata"));

    @SuppressWarnings("unchecked")
    final PCollection<KV<BucketShardId, ResourceId>> writtenFiles =
        (PCollection<KV<BucketShardId, ResourceId>>)
            writeResult.expand().get(new TupleTag<>("WrittenFiles"));

    PAssert.thatSingleton(writtenMetadata)
        .satisfies(
            id -> {
              Assert.assertTrue(readMetadata(id).isCompatibleWith(metadata));
              return null;
            });

    PAssert.thatMap(writtenFiles)
        .satisfies(
            (m) -> {
              final Map<BucketShardId, List<String>> data =
                  m.entrySet().stream()
                      .collect(Collectors.toMap(Map.Entry::getKey, e -> readFile(e.getValue())));
              checkFn.accept(data);
              return null;
            });
  }

  private static BucketMetadata<String, String> readMetadata(ResourceId file) {
    try {
      return BucketMetadata.from(Channels.newInputStream(FileSystems.open(file)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> readFile(ResourceId file) {
    try {
      return CharStreams.readLines(
          new InputStreamReader(
              Channels.newInputStream(FileSystems.open(file)), Charset.defaultCharset()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class ExceptionThrowingFileOperations extends FileOperations<String> {
    ExceptionThrowingFileOperations() {
      super(Compression.UNCOMPRESSED, MimeTypes.TEXT);
    }

    @Override
    protected Reader<String> createReader() {
      throw new RuntimeException("Not implemented");
    }

    @Override
    protected Sink<String> createSink() {
      throw new RuntimeException();
    }

    @Override
    public Coder<String> getCoder() {
      return StringUtf8Coder.of();
    }
  }

  private interface SerializableConsumer<T> extends Consumer<T>, Serializable {}

  private SerializableConsumer<Map<BucketShardId, List<String>>> assertValidSmbFormat(
      TestBucketMetadata metadata) {
    return writtenBuckets -> {
      final Map<String, Integer> keysToBuckets = new HashMap<>();
      final List<String> seenItems = new ArrayList<>();

      writtenBuckets.forEach(
          (bucketShardId, writtenRecords) -> {
            String prevKey = null;
            final Integer bucketId = bucketShardId.getBucketId();
            for (String record : writtenRecords) {
              seenItems.add(record);

              if (prevKey == null) {
                prevKey = metadata.extractKey(record);
                keysToBuckets.put(prevKey, bucketId);
                continue;
              }

              final String currKey = metadata.extractKey(record);
              Assert.assertEquals(
                  "Record " + record + " was not written to correct bucket",
                  bucketShardId.getBucketId(),
                  metadata.getBucketId(metadata.getKeyBytes(record)));

              Assert.assertTrue(
                  "Keys in " + bucketShardId + " are not in sorted order.",
                  prevKey.compareTo(currKey) <= 0);

              final Integer existingKeyBucket = keysToBuckets.get(currKey);
              Assert.assertTrue(
                  "Key in " + bucketShardId + " overlaps with bucket " + existingKeyBucket,
                  existingKeyBucket == null || existingKeyBucket.equals(bucketId));

              keysToBuckets.put(currKey, bucketId);
            }
          });

      MatcherAssert.assertThat(
          "Written items do not match PCollection input",
          seenItems,
          Matchers.containsInAnyOrder(input));

      final Set<BucketShardId> allBucketShardIds = new HashSet<>();
      for (int bucketId = 0; bucketId < metadata.getNumBuckets(); bucketId++) {
        for (int shardId = 0; shardId < metadata.getNumShards(); shardId++) {
          allBucketShardIds.add(BucketShardId.of(bucketId, shardId));
        }
      }
      Assert.assertEquals(
          "Written bucketShardIds did not match metadata",
          allBucketShardIds,
          writtenBuckets.keySet());
    };
  }
}
