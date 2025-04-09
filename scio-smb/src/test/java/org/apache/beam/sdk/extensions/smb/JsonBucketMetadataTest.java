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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link JsonBucketMetadata}. */
public class JsonBucketMetadataTest {

  @Test
  public void testExtractKey() throws Exception {
    final TableRow user =
        new TableRow()
            .set("user", "Alice")
            .set("age", 10)
            .set(
                "location",
                new TableRow()
                    .set("currentCountry", "US")
                    .set("prevCountries", Arrays.asList("CN", "MX")));

    BucketMetadata<Integer, String, TableRow> metadata =
        new JsonBucketMetadata<>(
            1,
            1,
            Integer.class,
            "age",
            String.class,
            "user",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    Assert.assertEquals((Integer) 10, metadata.extractKeyPrimary(user));
    Assert.assertEquals("Alice", metadata.extractKeySecondary(user));

    Assert.assertEquals(
        "US",
        new JsonBucketMetadata<>(
                1,
                1,
                String.class,
                "location.currentCountry",
                null,
                null,
                HashType.MURMUR3_32,
                SortedBucketIO.DEFAULT_FILENAME_PREFIX)
            .extractKeyPrimary(user));

    /*
    FIXME: BucketMetadata should allow custom coder?
    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        new JsonBucketMetadata<>(
                1, 1, ArrayList.class, HashType.MURMUR3_32, "location.prevCountries")
            .extractKeyPrimary(user));
     */
  }

  @Test
  public void testCoding() throws Exception {
    final JsonBucketMetadata<String, Integer> metadata =
        new JsonBucketMetadata<>(
            1,
            1,
            1,
            String.class,
            "favorite_color",
            Integer.class,
            "favorite_number",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final BucketMetadata<String, Integer, TableRow> copy = BucketMetadata.from(metadata.toString());
    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getKeyClassSecondary(), copy.getKeyClassSecondary());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
  }

  @Test
  public void testVersionDefault() throws Exception {
    final JsonBucketMetadata<String, Void> metadata =
        new JsonBucketMetadata<>(
            1,
            1,
            String.class,
            "favorite_color",
            null,
            null,
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    Assert.assertEquals(BucketMetadata.CURRENT_VERSION, metadata.getVersion());
  }

  @Test
  public void testDisplayData() throws Exception {
    final JsonBucketMetadata<String, Integer> metadata =
        new JsonBucketMetadata<>(
            2,
            1,
            String.class,
            "favorite_color",
            Integer.class,
            "favorite_number",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final DisplayData displayData = DisplayData.from(metadata);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("version", BucketMetadata.CURRENT_VERSION));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyFieldPrimary", "favorite_color"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassPrimary", String.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoderPrimary", StringUtf8Coder.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyFieldSecondary", "favorite_number"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClassSecondary", Integer.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoderSecondary", VarIntCoder.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
  }

  @Test
  public void testSameSourceCompatibility() throws Exception {
    final JsonBucketMetadata<String, String> metadata1 =
        new JsonBucketMetadata<>(
            2,
            1,
            String.class,
            "favorite_country",
            String.class,
            "favorite_color",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final JsonBucketMetadata<String, String> metadata2 =
        new JsonBucketMetadata<>(
            2,
            1,
            String.class,
            "favorite_color",
            String.class,
            "favorite_country",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final JsonBucketMetadata<String, String> metadata3 =
        new JsonBucketMetadata<>(
            4,
            1,
            String.class,
            "favorite_color",
            String.class,
            "favorite_country",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final JsonBucketMetadata<Long, Long> metadata4 =
        new JsonBucketMetadata<>(
            4,
            1,
            Long.class,
            "favorite_color",
            Long.class,
            "favorite_country",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final JsonBucketMetadata<Long, String> metadata5 =
        new JsonBucketMetadata<>(
            4,
            1,
            Long.class,
            "favorite_color",
            String.class,
            "favorite_country",
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

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
  public void skipsNullSecondaryKeys()
      throws CannotProvideCoderException, Coder.NonDeterministicException, IOException {
    final JsonBucketMetadata<String, Void> metadata =
        new JsonBucketMetadata<>(
            4,
            1,
            String.class,
            "favorite_color",
            null,
            null,
            HashType.MURMUR3_32,
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    BucketMetadata.to(metadata, os);

    Assert.assertFalse(os.toString().contains("keyFieldSecondary"));
    Assert.assertNull(
        ((JsonBucketMetadata) BucketMetadata.from(os.toString())).getKeyClassSecondary());
  }
}
