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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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

  @BeforeClass
  public static void writeData() throws Exception {
    sinkPipeline
        .apply("CreateLHS", Create.of(inputLhs))
        .apply("SinkLHS", new SortedBucketSink<>(
            TestBucketMetadata.of(4, 3), fromFolder(inputLhsFolder), fromFolder(tempFolder), ".txt", new TestFileOperations(), 1)
        );

    sinkPipeline
        .apply("CreateRHS", Create.of(inputRhs))
        .apply("SinkRHS", new SortedBucketSink<>(
            TestBucketMetadata.of(1, 1), fromFolder(inputRhsFolder), fromFolder(tempFolder), ".txt", new TestFileOperations(), 1)
        );

    sinkPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSortedBucketTransform() throws Exception {
    final TestBucketMetadata outputMetadata = TestBucketMetadata.of(1, 1);
    final List<BucketedInput<?, ?>> sources = ImmutableList.of(
        new BucketedInput<String, String>(
            new TupleTag<>("lhs"),
            fromFolder(inputLhsFolder),
            ".txt",
            new TestFileOperations()
        ),
        new BucketedInput<String, String>(
            new TupleTag<>("rhs"),
            fromFolder(inputRhsFolder),
            ".txt",
            new TestFileOperations()
        ));

    final SerializableFunction<KV<String, CoGbkResult>, Iterator<String>> transformFn =
        (SerializableFunction<KV<String, CoGbkResult>, Iterator<String>>) input -> {
          final List<String> output = new ArrayList<>();

          input.getValue().getAll(new TupleTag<String>("lhs")).forEach(lhs -> {
            input.getValue().getAll(new TupleTag<String>("rhs")).forEach(rhs -> {
              output.add(lhs + "-" + rhs);
            });
          });

          return output.iterator();
        };

    transformPipeline.apply(
        new SortedBucketTransform<>(
            String.class,
            outputMetadata,
            fromFolder(outputFolder),
            fromFolder(tempFolder),
            ".txt",
            new TestFileOperations(),
            sources,
            transformFn
        )
    );

    transformPipeline.run().waitUntilFinish();

    final FileOperations.Reader<String> outputReader = new TestFileOperations().createReader();
    outputReader.prepareRead(
        FileSystems.open(new SMBFilenamePolicy(fromFolder(outputFolder), ".txt")
            .forDestination()
            .forBucket(BucketShardId.of(0, 0), outputMetadata)
    ));

    final List<String> outputElements = ImmutableList.copyOf(outputReader.iterator());
    final List<String> expected = ImmutableList.of("c1-c2", "d1-d2", "e1-e2");

    Assert.assertEquals(expected, outputElements);
  }

  @Test
  public void testFailsOnIncompatibleMetadata() throws Exception {
    // # of buckets requested for output > least number of buckets in input
    final TestBucketMetadata outputMetadata = TestBucketMetadata.of(8, 1);
    final List<BucketedInput<?, ?>> sources = Collections.singletonList(
        new BucketedInput<String, String>(
            new TupleTag<>("lhs"),
            fromFolder(inputLhsFolder),
            ".txt",
            new TestFileOperations())
    );

    final SerializableFunction<KV<String, CoGbkResult>, Iterator<String>> transformFn =
        (input) -> { throw new RuntimeException("This code is unreachable"); };

    Assert.assertThrows(
        "Specified number of buckets 8 does not match smallest bucket size among inputs: 4",
        IllegalArgumentException.class,
        () ->
          new SortedBucketTransform<>(
              String.class,
              outputMetadata,
              fromFolder(outputFolder),
              fromFolder(tempFolder),
              ".txt",
              new TestFileOperations(),
              sources,
              transformFn
          ).expand(PBegin.in(transformPipeline))
      );
  }
}
