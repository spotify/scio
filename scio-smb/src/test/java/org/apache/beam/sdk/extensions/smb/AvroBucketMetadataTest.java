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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link AvroBucketMetadata}. */
public class AvroBucketMetadataTest {

  static final Schema LOCATION_SCHEMA =
      Schema.createRecord(
          "Location",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("countryId", Schema.create(Type.BYTES), "", ""),
              new Schema.Field(
                  "postalCode",
                  Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BYTES)),
                  "",
                  ""),
              new Schema.Field(
                  "prevCountries",
                  Schema.createArray(Schema.create(Schema.Type.STRING)),
                  "",
                  Collections.<String>emptyList())));

  static final Schema LOCATION_UNION_SCHEMA =
      Schema.createUnion(Schema.create(Type.NULL), LOCATION_SCHEMA);

  static final Schema RECORD_SCHEMA =
      Schema.createRecord(
          "Record",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("id", Schema.create(Schema.Type.LONG), "", 0L),
              new Schema.Field("location", LOCATION_SCHEMA, "", Collections.emptyList()),
              new Schema.Field("locationUnion", LOCATION_UNION_SCHEMA, "", Collections.emptyList()),
              new Schema.Field(
                  "suffix",
                  Schema.createEnum("Suffix", "", "", Lists.newArrayList("Jr", "Sr", "None")),
                  "",
                  "None")));

  @Test
  public void testGenericRecord() throws Exception {
    final ByteBuffer countryIdAsBytes = ByteBuffer.wrap("US".getBytes(Charset.defaultCharset()));
    final ByteBuffer postalCodeBytes = ByteBuffer.wrap("11".getBytes(Charset.defaultCharset()));
    final GenericRecord location =
        new GenericRecordBuilder(LOCATION_SCHEMA)
            .set("countryId", countryIdAsBytes)
            .set("prevCountries", Arrays.asList("CN", "MX"))
            .set("postalCode", postalCodeBytes)
            .build();

    final GenericRecord user =
        new GenericRecordBuilder(RECORD_SCHEMA)
            .set("id", 10L)
            .set("location", location)
            .set("locationUnion", location)
            .set("suffix", "Jr")
            .build();

    Assert.assertEquals(
        (Long) 10L,
        new AvroBucketMetadata<>(
                1,
                1,
                Long.class,
                HashType.MURMUR3_32,
                "id",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA)
            .extractKey(user));

    Assert.assertEquals(
        countryIdAsBytes,
        new AvroBucketMetadata<>(
                1,
                1,
                ByteBuffer.class,
                HashType.MURMUR3_32,
                "location.countryId",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA)
            .extractKey(user));

    Assert.assertEquals(
        countryIdAsBytes,
        new AvroBucketMetadata<>(
                1,
                1,
                ByteBuffer.class,
                HashType.MURMUR3_32,
                "locationUnion.countryId",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA)
            .extractKey(user));

    Assert.assertEquals(
        postalCodeBytes,
        new AvroBucketMetadata<>(
                1,
                1,
                ByteBuffer.class,
                HashType.MURMUR3_32,
                "locationUnion.postalCode",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA)
            .extractKey(user));

    Assert.assertEquals(
        "Jr",
        new AvroBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "suffix",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA)
            .extractKey(user));

    /*
    FIXME: BucketMetadata should allow custom coder?
    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        new AvroBucketMetadata<>(
                1, 1, ArrayList.class, HashType.MURMUR3_32, "location.prevCountries")
            .extractKey(user));
     */
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final AvroGeneratedUser user = new AvroGeneratedUser("foo", 50, "green");

    Assert.assertEquals(
        "green",
        new AvroBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "favorite_color",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                AvroGeneratedUser.class)
            .extractKey(user));

    Assert.assertEquals(
        (Integer) 50,
        new AvroBucketMetadata<>(
                1,
                1,
                Integer.class,
                HashType.MURMUR3_32,
                "favorite_number",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                AvroGeneratedUser.class)
            .extractKey(user));
  }

  @Test
  public void testCoding() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(
            1,
            1,
            1,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX);

    final BucketMetadata<String, GenericRecord> copy = BucketMetadata.from(metadata.toString());
    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
  }

  @Test
  public void testVersionDefault() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(
            1,
            1,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    Assert.assertEquals(BucketMetadata.CURRENT_VERSION, metadata.getVersion());
  }

  @Test
  public void testDisplayData() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(
            2,
            1,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    final DisplayData displayData = DisplayData.from(metadata);
    MatcherAssert.assertThat(displayData, hasDisplayItem("numBuckets", 2));
    MatcherAssert.assertThat(displayData, hasDisplayItem("numShards", 1));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("version", BucketMetadata.CURRENT_VERSION));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyField", "favorite_color"));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyClass", String.class));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("hashType", HashType.MURMUR3_32.toString()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("keyCoder", StringUtf8Coder.class));
  }

  @Test
  public void testSameSourceCompatibility() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata1 =
        new AvroBucketMetadata<>(
            2,
            1,
            String.class,
            HashType.MURMUR3_32,
            "name",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<String, GenericRecord> metadata2 =
        new AvroBucketMetadata<>(
            2,
            1,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<String, GenericRecord> metadata3 =
        new AvroBucketMetadata<>(
            4,
            1,
            String.class,
            HashType.MURMUR3_32,
            "favorite_color",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<Integer, GenericRecord> metadata4 =
        new AvroBucketMetadata<>(
            4,
            1,
            Integer.class,
            HashType.MURMUR3_32,
            "favorite_number",
            SortedBucketIO.DEFAULT_FILENAME_PREFIX,
            AvroGeneratedUser.SCHEMA$);

    Assert.assertFalse(metadata1.isPartitionCompatible(metadata2));
    Assert.assertTrue(metadata2.isPartitionCompatible(metadata3));
    Assert.assertFalse(metadata3.isPartitionCompatible(metadata4));
  }

  @Test
  public void testKeyTypeCheckingBytes()
      throws CannotProvideCoderException, NonDeterministicException {
    new AvroBucketMetadata<>(
        1,
        1,
        ByteBuffer.class,
        HashType.MURMUR3_32,
        "location.countryId",
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        RECORD_SCHEMA);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new AvroBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "location.countryId",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                RECORD_SCHEMA));
  }

  @Test
  public void testKeyTypeCheckingUnionTypes()
      throws CannotProvideCoderException, NonDeterministicException {
    final Schema legalUnionSchema = createUnionRecordOfTypes(Type.STRING, Type.NULL);
    // Two types, one of which isn't a null
    final Schema illegalUnionSchema1 = createUnionRecordOfTypes(Type.STRING, Type.BYTES);
    // Three types
    final Schema illegalUnionSchema2 = createUnionRecordOfTypes(Type.STRING, Type.BYTES, Type.NULL);

    new AvroBucketMetadata<>(
        1,
        1,
        String.class,
        HashType.MURMUR3_32,
        "unionField",
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        legalUnionSchema);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new AvroBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "unionField",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                illegalUnionSchema1));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            new AvroBucketMetadata<>(
                1,
                1,
                String.class,
                HashType.MURMUR3_32,
                "unionField",
                SortedBucketIO.DEFAULT_FILENAME_PREFIX,
                illegalUnionSchema2));
  }

  private static Schema createUnionRecordOfTypes(Schema.Type... types) {
    final List<Schema> typeSchemas = new ArrayList<>();
    Arrays.asList(types).forEach(t -> typeSchemas.add(Schema.create(t)));
    return Schema.createRecord(
        "Record",
        "",
        "org.apache.beam.sdk.extensions.smb.avro",
        false,
        Lists.newArrayList(
            new Schema.Field("unionField", Schema.createUnion(typeSchemas), "", "")));
  }
}
