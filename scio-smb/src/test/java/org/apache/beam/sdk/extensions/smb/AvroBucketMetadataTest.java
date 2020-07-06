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

import com.google.protobuf.ByteString;
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

  private static final Schema LOCATION_SCHEMA =
      Schema.createRecord(
          "Location",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("countryId", Schema.create(Type.BYTES), "", ""),
              new Schema.Field(
                  "prevCountries",
                  Schema.createArray(Schema.create(Schema.Type.STRING)),
                  "",
                  Collections.<String>emptyList())));

  private static final Schema RECORD_SCHEMA =
      Schema.createRecord(
          "Record",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("id", Schema.create(Schema.Type.LONG), "", 0L),
              new Schema.Field("location", LOCATION_SCHEMA, "", Collections.emptyList())));

  @Test
  public void testGenericRecord() throws Exception {
    final ByteBuffer countryIdAsBytes = ByteBuffer.wrap("US".getBytes(Charset.defaultCharset()));
    final GenericRecord location =
        new GenericRecordBuilder(LOCATION_SCHEMA)
            .set("countryId", countryIdAsBytes)
            .set("prevCountries", Arrays.asList("CN", "MX"))
            .build();

    final GenericRecord user =
        new GenericRecordBuilder(RECORD_SCHEMA).set("id", 10L).set("location", location).build();

    Assert.assertEquals(
        (Long) 10L,
        AvroBucketMetadata.of(1, 1, Long.class, HashType.MURMUR3_32, "id", RECORD_SCHEMA)
            .extractKey(user));

    Assert.assertEquals(
        countryIdAsBytes,
        AvroBucketMetadata.of(
                1, 1, ByteBuffer.class, HashType.MURMUR3_32, "location.countryId", RECORD_SCHEMA)
            .extractKey(user));

    /*
    FIXME: BucketMetadata should allow custom coder?
    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        AvroBucketMetadata.of(
                1, 1, ArrayList.class, HashType.MURMUR3_32, "location.prevCountries")
            .extractKey(user));
     */
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final AvroGeneratedUser user = new AvroGeneratedUser("foo", 50, "green");

    Assert.assertEquals(
        "green",
        AvroBucketMetadata.of(
                1, 1, String.class, HashType.MURMUR3_32, "favorite_color", AvroGeneratedUser.class)
            .extractKey(user));

    Assert.assertEquals(
        (Integer) 50,
        AvroBucketMetadata.of(
                1,
                1,
                Integer.class,
                HashType.MURMUR3_32,
                "favorite_number",
                AvroGeneratedUser.class)
            .extractKey(user));
  }

  @Test
  public void testCoding() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, 1, 1, String.class, HashType.MURMUR3_32, "favorite_color");

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
        AvroBucketMetadata.of(
            1, 1, String.class, HashType.MURMUR3_32, "favorite_color", AvroGeneratedUser.SCHEMA$);

    Assert.assertEquals(BucketMetadata.CURRENT_VERSION, metadata.getVersion());
  }

  @Test
  public void testDisplayData() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        AvroBucketMetadata.of(
            2, 1, String.class, HashType.MURMUR3_32, "favorite_color", AvroGeneratedUser.SCHEMA$);

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
        AvroBucketMetadata.of(
            2, 1, String.class, HashType.MURMUR3_32, "name", AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<String, GenericRecord> metadata2 =
        AvroBucketMetadata.of(
            2, 1, String.class, HashType.MURMUR3_32, "favorite_color", AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<String, GenericRecord> metadata3 =
        AvroBucketMetadata.of(
            4, 1, String.class, HashType.MURMUR3_32, "favorite_color", AvroGeneratedUser.SCHEMA$);

    final AvroBucketMetadata<Integer, GenericRecord> metadata4 =
        AvroBucketMetadata.of(
            4, 1, Integer.class, HashType.MURMUR3_32, "favorite_number", AvroGeneratedUser.SCHEMA$);

    Assert.assertFalse(metadata1.isPartitionCompatible(metadata2));
    Assert.assertTrue(metadata2.isPartitionCompatible(metadata3));
    Assert.assertFalse(metadata3.isPartitionCompatible(metadata4));
  }

  @Test
  public void testKeyTypeCheckingBytes()
      throws CannotProvideCoderException, NonDeterministicException {
    AvroBucketMetadata.of(
        1, 1, byte[].class, HashType.MURMUR3_32, "location.countryId", RECORD_SCHEMA);

    AvroBucketMetadata.of(
        1, 1, ByteString.class, HashType.MURMUR3_32, "location.countryId", RECORD_SCHEMA);

    AvroBucketMetadata.of(
        1, 1, ByteBuffer.class, HashType.MURMUR3_32, "location.countryId", RECORD_SCHEMA);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroBucketMetadata.of(
                1, 1, String.class, HashType.MURMUR3_32, "location.countryId", RECORD_SCHEMA));
  }

  @Test
  public void testKeyTypeCheckingEnum()
      throws CannotProvideCoderException, NonDeterministicException {
    final Schema enumSchema =
        Schema.createRecord(
            "enumSchema",
            "",
            "",
            false,
            Lists.newArrayList(
                new Schema.Field(
                    "enumField",
                    Schema.createEnum("enumType", "", "", Lists.newArrayList("a", "b", "c")),
                    "",
                    "a")));

    AvroBucketMetadata.of(1, 1, String.class, HashType.MURMUR3_32, "enumField", enumSchema);
  }

  @Test
  public void testKeyTypeCheckingUnionTypes()
      throws CannotProvideCoderException, NonDeterministicException {
    final Schema legalUnionSchema = createUnionRecordOfTypes(Type.STRING, Type.NULL);
    // Two types, one of which isn't a null
    final Schema illegalUnionSchema1 = createUnionRecordOfTypes(Type.STRING, Type.BYTES);
    // Three types
    final Schema illegalUnionSchema2 = createUnionRecordOfTypes(Type.STRING, Type.BYTES, Type.NULL);

    AvroBucketMetadata.of(1, 1, String.class, HashType.MURMUR3_32, "unionField", legalUnionSchema);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroBucketMetadata.of(
                1, 1, String.class, HashType.MURMUR3_32, "unionField", illegalUnionSchema1));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroBucketMetadata.of(
                1, 1, String.class, HashType.MURMUR3_32, "unionField", illegalUnionSchema2));
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
