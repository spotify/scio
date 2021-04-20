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
import java.nio.charset.Charset;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
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

    Assert.assertArrayEquals(
        "data".getBytes(Charset.defaultCharset()),
        new TensorFlowBucketMetadata<>(
                1,
                1,
                byte[].class,
                HashType.MURMUR3_32,
                "bytes",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKey(example));

    Assert.assertEquals(
        ByteString.copyFrom("data".getBytes()),
        new TensorFlowBucketMetadata<>(
                1,
                1,
                ByteString.class,
                HashType.MURMUR3_32,
                "bytes",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKey(example));

    Assert.assertEquals(
        "data",
        new TensorFlowBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "bytes",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKey(example));

    Assert.assertEquals(
        (Long) 12345L,
        new TensorFlowBucketMetadata<>(
                1,
                1,
                Long.class,
                HashType.MURMUR3_32,
                "int",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKey(example));

    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new TensorFlowBucketMetadata<>(
                    1,
                    1,
                    Float.class,
                    HashType.MURMUR3_32,
                    "float",
                    SortedBucketIO.DEFAULT_FILENAME_PREFIX)
                .extractKey(example));

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            new TensorFlowBucketMetadata<>(
                    1,
                    1,
                    Integer.class,
                    HashType.MURMUR3_32,
                    "bytes",
                    SortedBucketIO.DEFAULT_FILENAME_PREFIX)
                .extractKey(example));
  }

  @Test
  public void testDisplayData() throws Exception {
    final TensorFlowBucketMetadata<byte[]> metadata =
        new TensorFlowBucketMetadata<>(
            2,
            1,
            byte[].class,
            HashType.MURMUR3_32,
            "bytes",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final DisplayData displayData = DisplayData.from(metadata);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("version", BucketMetadata.CURRENT_VERSION));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyField", "bytes"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClass", byte[].class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoder", ByteArrayCoder.class));
  }

  @Test
  public void testSameSourceCompatibility() throws Exception {
    final TensorFlowBucketMetadata<byte[]> metadata1 =
        new TensorFlowBucketMetadata<>(
            2, 1, byte[].class, HashType.MURMUR3_32, "foo", SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final TensorFlowBucketMetadata<byte[]> metadata2 =
        new TensorFlowBucketMetadata<>(
            2, 1, byte[].class, HashType.MURMUR3_32, "bar", SortedBucketIO.DEFAULT_FILENAME_PREFIX);
    ;

    final TensorFlowBucketMetadata<byte[]> metadata3 =
        new TensorFlowBucketMetadata<>(
            4, 1, byte[].class, HashType.MURMUR3_32, "bar", SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final TensorFlowBucketMetadata<String> metadata4 =
        new TensorFlowBucketMetadata<>(
            4, 1, String.class, HashType.MURMUR3_32, "bar", SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    Assert.assertFalse(metadata1.isPartitionCompatible(metadata2));
    Assert.assertTrue(metadata2.isPartitionCompatible(metadata3));
    Assert.assertFalse(metadata3.isPartitionCompatible(metadata4));
  }
}
