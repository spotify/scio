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
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

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
        new TensorFlowBucketMetadata<>(1, 1, byte[].class, HashType.MURMUR3_32, "bytes")
            .extractKey(example));

    Assert.assertEquals(
        (Long) 12345L,
        new TensorFlowBucketMetadata<>(1, 1, Long.class, HashType.MURMUR3_32, "int")
            .extractKey(example));

    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new TensorFlowBucketMetadata<>(1, 1, Float.class, HashType.MURMUR3_32, "float")
                .extractKey(example));

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            new TensorFlowBucketMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "bytes")
                .extractKey(example));
  }

  @Test
  public void testDisplayData() throws Exception {
    final TensorFlowBucketMetadata<byte[]> metadata =
        new TensorFlowBucketMetadata<>(2, 1, byte[].class, HashType.MURMUR3_32, "bytes");

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
        new TensorFlowBucketMetadata<>(2, 1, byte[].class, HashType.MURMUR3_32, "foo");

    final TensorFlowBucketMetadata<byte[]> metadata2 =
        new TensorFlowBucketMetadata<>(2, 1, byte[].class, HashType.MURMUR3_32, "bar");;

    final TensorFlowBucketMetadata<byte[]> metadata3 =
        new TensorFlowBucketMetadata<>(4, 1, byte[].class, HashType.MURMUR3_32, "bar");

    final TensorFlowBucketMetadata<String> metadata4 =
        new TensorFlowBucketMetadata<>(4, 1, String.class, HashType.MURMUR3_32, "bar");

    Assert.assertFalse(metadata1.isSameSourceCompatible(metadata2));
    Assert.assertTrue(metadata2.isSameSourceCompatible(metadata3));
    Assert.assertFalse(metadata3.isSameSourceCompatible(metadata4));
  }
}
