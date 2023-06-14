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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link AvroFileOperations}. */
public class AvroFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  private static final Schema USER_SCHEMA =
      Schema.createRecord(
          "User",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("name", Schema.create(Schema.Type.STRING), "", ""),
              new Schema.Field("age", Schema.create(Schema.Type.INT), "", 0)));

  private static final Map<String, Object> TEST_METADATA = ImmutableMap.of("foo", "bar");

  @Test
  public void testGenericRecord() throws Exception {
    final AvroFileOperations<GenericRecord> fileOperations =
        AvroFileOperations.of(USER_SCHEMA, CodecFactory.snappyCodec(), TEST_METADATA);
    final ResourceId file =
        fromFolder(output).resolve("file.avro", StandardResolveOptions.RESOLVE_FILE);

    final List<GenericRecord> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    new GenericRecordBuilder(USER_SCHEMA)
                        .set("name", String.format("user%02d", i))
                        .set("age", i)
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<GenericRecord> writer = fileOperations.createWriter(file);
    for (GenericRecord record : records) {
      writer.write(record);
    }
    writer.close();

    assertMetadata(file, TEST_METADATA);

    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final AvroFileOperations<AvroGeneratedUser> fileOperations =
        AvroFileOperations.of(AvroGeneratedUser.class, CodecFactory.snappyCodec(), TEST_METADATA);
    final ResourceId file =
        fromFolder(output).resolve("file.avro", StandardResolveOptions.RESOLVE_FILE);

    final List<AvroGeneratedUser> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    AvroGeneratedUser.newBuilder()
                        .setName(String.format("user%02d", i))
                        .setFavoriteColor(String.format("color%02d", i))
                        .setFavoriteNumber(i)
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<AvroGeneratedUser> writer = fileOperations.createWriter(file);
    for (AvroGeneratedUser record : records) {
      writer.write(record);
    }
    writer.close();

    assertMetadata(file, TEST_METADATA);

    final List<AvroGeneratedUser> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }

  @Test
  public void testDisplayData() {
    final AvroFileOperations<AvroGeneratedUser> fileOperations =
        AvroFileOperations.of(AvroGeneratedUser.class);

    final DisplayData displayData = DisplayData.from(fileOperations);
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("FileOperations", AvroFileOperations.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("mimeType", MimeTypes.BINARY));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("compression", Compression.UNCOMPRESSED.toString()));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("codecFactory", CodecFactory.deflateCodec(6).getClass()));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("schema", AvroGeneratedUser.SCHEMA$.getFullName()));
  }

  // https://github.com/spotify/scio/issues/2649
  @Test
  public void testMap2649() throws Exception {
    final Schema schema =
        Schema.createRecord(
            "Record",
            "",
            "org.apache.beam.sdk.extensions.smb.avro",
            false,
            Lists.newArrayList(
                new Schema.Field(
                    "map",
                    Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createMap(Schema.create(Schema.Type.STRING))),
                    "",
                    JsonProperties.NULL_VALUE)));

    final AvroFileOperations<GenericRecord> fileOperations = AvroFileOperations.of(schema);
    final ResourceId file =
        fromFolder(output).resolve("map2649.avro", StandardResolveOptions.RESOLVE_FILE);

    // String round-trips back as Utf8, causing the map to be treated as non-string-map in
    // ReflectData.isNonStringMap
    final GenericRecord record =
        CoderUtils.clone(
            AvroCoder.of(schema),
            new GenericRecordBuilder(schema)
                .set("map", Collections.singletonMap("key", "value"))
                .build());

    final FileOperations.Writer<GenericRecord> writer = fileOperations.createWriter(file);
    writer.write(record);
    writer.close();

    GenericRecord actual = fileOperations.iterator(file).next();
    Assert.assertEquals(record, actual);
  }

  private void assertMetadata(ResourceId file, Map<String, Object> expected) throws Exception {
    final DataFileStream<GenericRecord> dfs =
        new DataFileStream<>(
            Channels.newInputStream(FileSystems.open(file)), new GenericDatumReader<>());

    expected.forEach((k, v) -> Assert.assertEquals(v, dfs.getMetaString(k)));
    dfs.close();
  }
}
