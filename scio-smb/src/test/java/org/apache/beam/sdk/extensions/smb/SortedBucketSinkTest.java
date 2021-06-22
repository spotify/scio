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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.SortedBucketPreKeyedSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
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
              Stream.concat(
                      IntStream.rangeClosed('a', 'z').boxed(),
                      IntStream.rangeClosed('A', 'Z').boxed())
                  .flatMap(
                      i ->
                          IntStream.rangeClosed(1, 10)
                              .boxed()
                              .map(
                                  j ->
                                      String.format(
                                          "%s%02d", String.valueOf((char) i.intValue()), j))),
              Stream.of("") // Include an element with a null key
              )
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

  @SuppressWarnings("unchecked")
  @Test
  @Category(NeedsRunner.class)
  public void testCleansUpTempFiles() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(1, 1);

    final File output = Files.createTempDirectory("output").toFile();
    final File temp = Files.createTempDirectory("temp").toFile();
    output.deleteOnExit();
    temp.deleteOnExit();

    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata,
            LocalResources.fromFile(output, true),
            LocalResources.fromFile(temp, true),
            ".txt",
            new TestFileOperations(),
            1,
            0);

    pipeline
        .apply("CleansUpTempFiles", Create.of(Stream.of(input).collect(Collectors.toList())))
        .apply(sink);
    pipeline.run().waitUntilFinish();

    // Assert that no files are left in the temp directory
    Assert.assertFalse(Files.walk(temp.toPath(), 2).anyMatch(path -> path.toFile().isFile()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCustomFilenamePrefix() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(1, 1, "custom-prefix");

    final ResourceId outputDirectory = fromFolder(output);
    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata, outputDirectory, fromFolder(temp), ".txt", new TestFileOperations(), 1);

    pipeline.apply("CustomFilenamePrefix", Create.empty(StringUtf8Coder.of())).apply(sink);
    pipeline.run().waitUntilFinish();

    final MatchResult outputFiles =
        FileSystems.match(
            TestUtils.fromFolder(output)
                .resolve("*-of-*.txt", StandardResolveOptions.RESOLVE_FILE)
                .toString());

    Assert.assertEquals(1, outputFiles.metadata().size());
    Assert.assertEquals(
        "custom-prefix-00000-of-00001.txt",
        outputFiles.metadata().get(0).resourceId().getFilename());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWritesEmptyBucketFiles() throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(2, 2);

    final ResourceId outputDirectory = fromFolder(output);
    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata, outputDirectory, fromFolder(temp), ".txt", new TestFileOperations(), 1);

    pipeline.apply("WritesEmptyBucketFiles", Create.empty(StringUtf8Coder.of())).apply(sink);
    pipeline.run().waitUntilFinish();

    final FileAssignment dstFiles =
        new SMBFilenamePolicy.FileAssignment(
            outputDirectory, SortedBucketIO.DEFAULT_FILENAME_PREFIX, ".txt", false);

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

    pipeline
        .apply(
            "WritesNoFilesIfPriorStepsFail",
            Create.of(Stream.of(input).collect(Collectors.toList())))
        .apply(sink);

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
            .apply(
                "test-" + UUID.randomUUID(),
                Create.of(Stream.of(input).collect(Collectors.toList())))
            .apply(reshuffle)
            .apply(sink),
        metadata,
        assertValidSmbFormat(metadata, input));

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

    final List<KV<String, String>> keyedInput =
        Stream.of(input).map(s -> KV.of(metadata.extractKey(s), s)).collect(Collectors.toList());

    check(
        pipeline
            .apply(
                "test-keyed-collection-" + UUID.randomUUID(),
                Create.of(keyedInput)
                    .withCoder(
                        KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), StringUtf8Coder.of())))
            .apply(reshuffle)
            .apply(sink),
        metadata,
        assertValidSmbFormat(metadata, input));

    pipeline.run();
  }

  static void check(
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
              Assert.assertTrue(m.containsKey(BucketShardId.ofNullKey()));
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

  interface SerializableConsumer<T> extends Consumer<T>, Serializable {}

  static SerializableConsumer<Map<BucketShardId, List<String>>> assertValidSmbFormat(
      TestBucketMetadata metadata, String[] expectedInput) {
    final int nullBucketId = BucketShardId.ofNullKey().getBucketId();

    return writtenBuckets -> {
      final Map<String, Integer> keysToBuckets = new HashMap<>();
      final List<String> seenItems = new ArrayList<>();

      writtenBuckets.forEach(
          (bucketShardId, writtenRecords) -> {
            String prevKey = "UNSET";
            final Integer bucketId = bucketShardId.getBucketId();
            for (String record : writtenRecords) {
              seenItems.add(record);

              if (prevKey.equals("UNSET")) {
                prevKey = metadata.extractKey(record);
                keysToBuckets.put(prevKey, bucketId);
                continue;
              }

              final String currKey = metadata.extractKey(record);
              Assert.assertEquals(
                  "Record " + record + " was not written to correct bucket",
                  bucketShardId.getBucketId(),
                  currKey == null
                      ? nullBucketId
                      : metadata.getBucketId(metadata.getKeyBytes(record)));

              Assert.assertTrue(
                  "Keys in " + bucketShardId + " are not in sorted order.",
                  currKey == null || prevKey.compareTo(currKey) <= 0);

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
          Matchers.containsInAnyOrder(expectedInput));

      Assert.assertTrue(
          "Written bucketShardIds did not match metadata",
          writtenBuckets.keySet().containsAll(metadata.getAllBucketShardIds()));
    };
  }
}
