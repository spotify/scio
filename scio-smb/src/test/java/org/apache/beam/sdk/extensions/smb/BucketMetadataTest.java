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

import static org.apache.beam.sdk.extensions.smb.SortedBucketIO.DEFAULT_FILENAME_PREFIX;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link BucketMetadata}. */
public class BucketMetadataTest {

  @Test
  public void testCoding() throws Exception {
    final BucketMetadata<String, String, String> metadata =
        new TestBucketMetadataWithSecondary(
            BucketMetadata.CURRENT_VERSION, 16, 4, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final BucketMetadata<String, String, String> copy = BucketMetadata.from(metadata.toString());

    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getKeyClassSecondary(), copy.getKeyClassSecondary());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
    Assert.assertEquals(metadata.getFilenamePrefix(), copy.getFilenamePrefix());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDeterminism() {
    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new BucketMetadata<Double, Void, Object>(
                BucketMetadata.CURRENT_VERSION,
                1,
                1,
                Double.class,
                null,
                HashType.MURMUR3_32,
                DEFAULT_FILENAME_PREFIX) {
              @Override
              public Double extractKeyPrimary(Object value) {
                return null;
              }

              @Override
              public Void extractKeySecondary(Object value) {
                return null;
              }

              @Override
              int hashPrimaryKeyMetadata() {
                return -1;
              }

              @Override
              int hashSecondaryKeyMetadata() {
                return -1;
              }
            });
  }

  @Test
  public void testSubTyping() throws Exception {
    final BucketMetadata<String, Void, String> test =
        new TestBucketMetadata(16, 4, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final BucketMetadata<String, String, String> testSecondary =
        new TestBucketMetadataWithSecondary(16, 4, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final BucketMetadata<String, Void, GenericRecord> avro =
        new AvroBucketMetadata<String, Void, GenericRecord>(
            16,
            4,
            String.class,
            "favorite_color",
            null,
            null,
            HashType.MURMUR3_32,
            DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);
    final BucketMetadata<String, Void, TableRow> json =
        new JsonBucketMetadata<>(
            16,
            4,
            String.class,
            "keyField",
            null,
            null,
            HashType.MURMUR3_32,
            DEFAULT_FILENAME_PREFIX);

    Assert.assertEquals(TestBucketMetadata.class, BucketMetadata.from(test.toString()).getClass());
    Assert.assertEquals(
        TestBucketMetadataWithSecondary.class,
        BucketMetadata.from(testSecondary.toString()).getClass());
    Assert.assertEquals(AvroBucketMetadata.class, BucketMetadata.from(avro.toString()).getClass());
    Assert.assertEquals(JsonBucketMetadata.class, BucketMetadata.from(json.toString()).getClass());
  }

  @Test
  public void testCompatibility() throws Exception {
    final TestBucketMetadata m1 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m2 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m3 =
        new TestBucketMetadata(0, 1, 2, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m4 =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_128, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m5 =
        new TestBucketMetadata(1, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    final TestBucketMetadata m6 =
        new TestBucketMetadata(2, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);

    Assert.assertTrue(m1.isCompatibleWith(m2));
    Assert.assertTrue(m1.isCompatibleWith(m3));
    Assert.assertFalse(m1.isCompatibleWith(m4));
    Assert.assertTrue("version 0 and version 1 should be compatible", m1.isCompatibleWith(m5));
    Assert.assertFalse(
        "version 0 and version 2 are presumed incompatible", m1.isCompatibleWith(m6));
    Assert.assertFalse(
        "version 1 and version 2 are presumed incompatible", m5.isCompatibleWith(m6));
  }

  @Test
  public void testDefaultFields() throws Exception {
    final String serializedAvro =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.AvroBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";
    final String serializedJson =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.JsonBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";
    final String serializedTensorflow =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.TensorFlowBucketMetadata\",\"version\":0,\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\"}";

    // Test that old BucketMetadata file format missing filenamePrefix will default to "bucket"
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((JsonBucketMetadata) BucketMetadata.from(serializedJson)).getFilenamePrefix());
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((AvroBucketMetadata) BucketMetadata.from(serializedAvro)).getFilenamePrefix());
    Assert.assertEquals(
        DEFAULT_FILENAME_PREFIX,
        ((TensorFlowBucketMetadata) BucketMetadata.from(serializedTensorflow)).getFilenamePrefix());

    // Test that key class secondary defaults to null when reading from version 0
    Assert.assertNull(
        ((JsonBucketMetadata) BucketMetadata.from(serializedJson)).getKeyClassSecondary());
    Assert.assertNull(
        ((AvroBucketMetadata) BucketMetadata.from(serializedAvro)).getKeyClassSecondary());
    Assert.assertNull(
        ((TensorFlowBucketMetadata) BucketMetadata.from(serializedTensorflow))
            .getKeyClassSecondary());
  }

  @Test
  public void testOldBucketMetadataIgnoresExtraFields() throws Exception {
    final int futureVersion = BucketMetadata.CURRENT_VERSION + 1;
    final String serializedAvro =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.AvroBucketMetadata\",\"version\":"
            + futureVersion
            + ",\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\", \"extra_field\":\"foo\"}";
    final String serializedJson =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.JsonBucketMetadata\",\"version\":"
            + futureVersion
            + ",\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\", \"extra_field\":\"foo\"}";
    final String serializedTensorflow =
        "{\"type\":\"org.apache.beam.sdk.extensions.smb.TensorFlowBucketMetadata\",\"version\":"
            + futureVersion
            + ",\"numBuckets\":2,\"numShards\":1,\"keyClass\":\"java.lang.String\",\"hashType\":\"MURMUR3_32\",\"keyField\":\"user_id\", \"extra_field\":\"foo\"}";

    // Assert that no exception is thrown decoding.
    Assert.assertEquals(
        ((JsonBucketMetadata) BucketMetadata.from(serializedJson)).getVersion(), futureVersion);
    Assert.assertEquals(
        ((AvroBucketMetadata) BucketMetadata.from(serializedAvro)).getVersion(), futureVersion);
    Assert.assertEquals(
        ((TensorFlowBucketMetadata) BucketMetadata.from(serializedTensorflow)).getVersion(),
        futureVersion);
  }

  @Test
  public void testNullKeyEncoding() throws Exception {
    final TestBucketMetadata m =
        new TestBucketMetadata(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    Assert.assertNull(m.extractKeyPrimary(""));
    Assert.assertNull(m.getKeyBytesPrimary(""));
    final TestBucketMetadataWithSecondary m2 =
        new TestBucketMetadataWithSecondary(0, 1, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);
    Assert.assertNull(m2.extractKeyPrimary(""));
    Assert.assertNull(m2.getKeyBytesPrimary(""));
    Assert.assertNull(m2.extractKeySecondary("a"));
    Assert.assertNull(m2.getKeyBytesSecondary("a"));
  }

  @Test
  public void testDisplayData() throws Exception {
    final TestBucketMetadataWithSecondary m =
        new TestBucketMetadataWithSecondary(3, 2, 1, HashType.MURMUR3_32, DEFAULT_FILENAME_PREFIX);

    final DisplayData displayData = DisplayData.from(m);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(displayData, hasDisplayItem("version", 3));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassPrimary", String.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassSecondary", String.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoderPrimary", StringUtf8Coder.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("keyCoderSecondary", StringUtf8Coder.class));
  }
}
