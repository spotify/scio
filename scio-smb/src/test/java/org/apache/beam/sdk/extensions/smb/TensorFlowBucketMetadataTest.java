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

import static org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Example;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.Features;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

/** Unit tests for {@link TensorFlowBucketMetadata}. */
public class TensorFlowBucketMetadataTest {

  @Test
  public void test() throws Exception {
    final Example example =
        Example.newBuilder()
            .setFeatures(
                Features.newBuilder()
                    .putFeature(
                        "bytes",
                        Feature.newBuilder()
                            .setBytesList(
                                BytesList.newBuilder()
                                    .addValue(ByteString.copyFromUtf8("data"))
                                    .build())
                            .build())
                    .putFeature(
                        "int",
                        Feature.newBuilder()
                            .setInt64List(Int64List.newBuilder().addValue(12345L).build())
                            .build())
                    .putFeature(
                        "float",
                        Feature.newBuilder()
                            .setFloatList(FloatList.newBuilder().addValue(1.2345f).build())
                            .build())
                    .build())
            .build();

    final TensorFlowBucketMetadata<byte[], Long> metadata1 =
        new TensorFlowBucketMetadata<>(
            1,
            1,
            byte[].class,
            "bytes",
            Long.class,
            "int",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    Assert.assertArrayEquals(
        "data".getBytes(Charset.defaultCharset()), metadata1.extractKeyPrimary(example));
    Assert.assertEquals((Long) 12345L, metadata1.extractKeySecondary(example));

    Assert.assertEquals(
        ByteString.copyFrom("data".getBytes()),
        new TensorFlowBucketMetadata<>(
                1,
                1,
                ByteString.class,
                "bytes",
                HashType.MURMUR3_32,
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKeyPrimary(example));

    Assert.assertEquals(
        "data",
        new TensorFlowBucketMetadata<>(
                1,
                1,
                String.class,
                "bytes",
                HashType.MURMUR3_32,
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKeyPrimary(example));

    final TensorFlowBucketMetadata<Long, String> metadata2 =
        new TensorFlowBucketMetadata<>(
            1,
            1,
            Long.class,
            "int",
            String.class,
            "bytes",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    Assert.assertEquals((Long) 12345L, metadata2.extractKeyPrimary(example));
    Assert.assertEquals("data", metadata2.extractKeySecondary(example));

    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new TensorFlowBucketMetadata<>(
                    1,
                    1,
                    Float.class,
                    "float",
                    HashType.MURMUR3_32,
                    SortedBucketIO.DEFAULT_FILENAME_PREFIX)
                .extractKeyPrimary(example));

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            new TensorFlowBucketMetadata<>(
                    1,
                    1,
                    Integer.class,
                    "bytes",
                    HashType.MURMUR3_32,
                    SortedBucketIO.DEFAULT_FILENAME_PREFIX)
                .extractKeyPrimary(example));
  }

  @Test
  public void testDisplayData() throws Exception {
    final TensorFlowBucketMetadata<byte[], String> metadata =
        new TensorFlowBucketMetadata<>(
            2,
            1,
            byte[].class,
            "bytes",
            String.class,
            "string",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final DisplayData displayData = DisplayData.from(metadata);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("version", BucketMetadata.CURRENT_VERSION));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyFieldPrimary", "bytes"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassPrimary", byte[].class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoderPrimary", ByteArrayCoder.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyFieldSecondary", "string"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassSecondary", String.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("keyCoderSecondary", StringUtf8Coder.class));
  }

  @Test
  public void testSameSourceCompatibility() throws Exception {
    final TensorFlowBucketMetadata<byte[], String> metadata1 =
        new TensorFlowBucketMetadata<>(
            2,
            1,
            byte[].class,
            "foo",
            String.class,
            "bar",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    final TensorFlowBucketMetadata<byte[], String> metadata2 =
        new TensorFlowBucketMetadata<>(
            2,
            1,
            byte[].class,
            "bar",
            String.class,
            "foo",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    ;
    final TensorFlowBucketMetadata<byte[], String> metadata3 =
        new TensorFlowBucketMetadata<>(
            4,
            1,
            byte[].class,
            "bar",
            String.class,
            "foo",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    final TensorFlowBucketMetadata<String, Long> metadata4 =
        new TensorFlowBucketMetadata<>(
            4,
            1,
            String.class,
            "bar",
            Long.class,
            "foo",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    final TensorFlowBucketMetadata<String, Void> metadata5 =
        new TensorFlowBucketMetadata<>(
            4, 1, String.class, "bar", HashType.MURMUR3_32, SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    Assert.assertFalse(metadata1.isPartitionCompatibleForPrimaryKey(metadata2));
    Assert.assertFalse(metadata1.isPartitionCompatibleForPrimaryAndSecondaryKey(metadata2));

    Assert.assertTrue(metadata2.isPartitionCompatibleForPrimaryKey(metadata3));
    Assert.assertTrue(metadata2.isPartitionCompatibleForPrimaryAndSecondaryKey(metadata3));

    Assert.assertFalse(metadata3.isPartitionCompatibleForPrimaryKey(metadata4));
    Assert.assertFalse(metadata3.isPartitionCompatibleForPrimaryAndSecondaryKey(metadata4));

    Assert.assertTrue(metadata4.isPartitionCompatibleForPrimaryKey(metadata5));
    Assert.assertFalse(metadata4.isPartitionCompatibleForPrimaryAndSecondaryKey(metadata5));
  }

  @Test
  public void skipsNullSecondaryKeys() throws CannotProvideCoderException, Coder.NonDeterministicException, IOException {
    final TensorFlowBucketMetadata<String, Void> metadata =
            new TensorFlowBucketMetadata<>(4, 1, String.class, "bar", HashType.MURMUR3_32, SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    BucketMetadata.to(metadata, os);

    Assert.assertFalse(os.toString().contains("keyFieldSecondary"));
    Assert.assertNull(((TensorFlowBucketMetadata) BucketMetadata.from(os.toString())).getKeyClassSecondary());
  }
}
