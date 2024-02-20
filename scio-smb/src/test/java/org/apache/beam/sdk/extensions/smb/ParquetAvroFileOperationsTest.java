/*
 * Copyright 2021 Spotify AB.
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

import com.spotify.scio.smb.AvroGeneratedUserProjection;
import com.spotify.scio.smb.TestLogicalTypes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.math.BigDecimal;

/** Unit tests for {@link ParquetAvroFileOperations}. */
public class ParquetAvroFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  private static final Schema USER_SCHEMA =
      SchemaBuilder.record("User")
          // intentionally set this namespace for testGenericRecord
          .namespace("org.apache.beam.sdk.extensions.smb.ParquetAvroFileOperationsTest$")
          .fields()
          .name("name")
          .type()
          .stringType()
          .stringDefault("")
          .name("age")
          .type()
          .intType()
          .intDefault(0)
          .endRecord();

  private static final List<GenericRecord> USER_RECORDS =
      IntStream.range(0, 10)
          .mapToObj(
              i ->
                  new GenericRecordBuilder(USER_SCHEMA)
                      .set("name", String.format("user%02d", i))
                      .set("age", i)
                      .build())
          .collect(Collectors.toList());

  // Intentionally avoid no-arg ctor to verify this class is not attempted to instantiate
  static class User {
    User(String str) {}
  }

  @Test
  public void testGenericRecord() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    writeFile(file);

    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA);

    Assert.assertEquals(fileOperations, SerializableUtils.ensureSerializable(fileOperations));

    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(USER_RECORDS, actual);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final ParquetAvroFileOperations<AvroGeneratedUser> fileOperations =
        ParquetAvroFileOperations.of(AvroGeneratedUser.class);

    Assert.assertEquals(fileOperations, SerializableUtils.ensureSerializable(fileOperations));

    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

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

    final List<AvroGeneratedUser> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }

  @Test
  public void testLogicalTypes() throws Exception {
    final Configuration conf = new Configuration();

    final ParquetAvroFileOperations<TestLogicalTypes> fileOperations =
        ParquetAvroFileOperations.of(TestLogicalTypes.class).withConfiguration(conf);

    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    final List<TestLogicalTypes> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    TestLogicalTypes.newBuilder()
                        .setTimestamp(DateTime.now())
                        .setDecimal(BigDecimal.decimal(1.0).setScale(2).bigDecimal())
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<TestLogicalTypes> writer = fileOperations.createWriter(file);
    for (TestLogicalTypes record : records) {
      writer.write(record);
    }
    writer.close();

    final List<TestLogicalTypes> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }

  @Test
  public void testGenericProjection() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    writeFile(file);

    final Schema projection =
        SchemaBuilder.record("UserProjection")
            .namespace("org.apache.beam.sdk.extensions.smb.avro")
            .fields()
            .requiredString("name")
            .endRecord();

    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA)
            .withCompression(CompressionCodecName.ZSTD)
            .withProjection(projection);

    Assert.assertEquals(fileOperations, SerializableUtils.ensureSerializable(fileOperations));

    final List<GenericRecord> expected =
        USER_RECORDS.stream()
            .map(r -> new GenericRecordBuilder(USER_SCHEMA).set("name", r.get("name")).build())
            .collect(Collectors.toList());
    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSpecificRecordWithProjection() throws Exception {
    final Schema projection =
        SchemaBuilder.record("AvroGeneratedUserProjection")
            .namespace("org.apache.beam.sdk.extensions.smb")
            .fields()
            .requiredString("name")
            .endRecord();

    final ParquetAvroFileOperations<AvroGeneratedUser> fileOperations =
        ParquetAvroFileOperations.of(AvroGeneratedUser.class).withProjection(projection);

    Assert.assertEquals(fileOperations, SerializableUtils.ensureSerializable(fileOperations));

    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

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

    final List<AvroGeneratedUser> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    final List<AvroGeneratedUser> expected =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    AvroGeneratedUser.newBuilder()
                        .setName(String.format("user%02d", i))
                        .setFavoriteColor(null)
                        .setFavoriteNumber(null)
                        .build())
            .collect(Collectors.toList());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testProjectedSpecificRecord() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    // Write records using full AvroGeneratedUser schema
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
    final FileOperations.Writer<AvroGeneratedUser> writer =
        ParquetAvroFileOperations.of(AvroGeneratedUser.class).createWriter(file);
    for (AvroGeneratedUser record : records) {
      writer.write(record);
    }
    writer.close();

    // Read records using generated AvroGeneratedUserProjection class
    final List<AvroGeneratedUserProjection> actual = new ArrayList<>();
    ParquetAvroFileOperations.of(AvroGeneratedUserProjection.class)
        .iterator(file)
        .forEachRemaining(actual::add);

    final List<AvroGeneratedUserProjection> expected =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    AvroGeneratedUserProjection.newBuilder()
                        .setName(String.format("user%02d", i))
                        .build())
            .collect(Collectors.toList());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPredicate() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    writeFile(file);

    final FilterPredicate predicate = FilterApi.ltEq(FilterApi.intColumn("age"), 5);

    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA).withFilterPredicate(predicate);

    Assert.assertEquals(fileOperations, SerializableUtils.ensureSerializable(fileOperations));

    final List<GenericRecord> expected =
        USER_RECORDS.stream().filter(r -> (int) r.get("age") <= 5).collect(Collectors.toList());
    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDisplayData() {
    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA);

    final DisplayData displayData = DisplayData.from(fileOperations);
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("FileOperations", ParquetAvroFileOperations.class));
    MatcherAssert.assertThat(displayData, hasDisplayItem("mimeType", MimeTypes.BINARY));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("compression", Compression.UNCOMPRESSED.toString()));
    MatcherAssert.assertThat(
        displayData, hasDisplayItem("compressionCodecName", CompressionCodecName.ZSTD.name()));
    MatcherAssert.assertThat(displayData, hasDisplayItem("schema", USER_SCHEMA.getFullName()));
  }

  @Test
  public void testConfigurationEquality() {
    final Configuration configuration1 = new Configuration();
    configuration1.set("foo", "bar");

    final ParquetAvroFileOperations<GenericRecord> fileOperations1 =
        ParquetAvroFileOperations.of(USER_SCHEMA).withConfiguration(configuration1);

    // Copy of configuration with same keys
    final Configuration configuration2 = new Configuration();
    configuration2.set("foo", "bar");

    final ParquetAvroFileOperations<GenericRecord> fileOperations2 =
        ParquetAvroFileOperations.of(USER_SCHEMA).withConfiguration(configuration2);

    // Assert that configuration equality check fails
    Assert.assertEquals(fileOperations1, SerializableUtils.ensureSerializable(fileOperations2));

    // Copy of configuration with different keys
    final Configuration configuration3 = new Configuration();
    configuration3.set("bar", "baz");

    final ParquetAvroFileOperations<GenericRecord> fileOperations3 =
        ParquetAvroFileOperations.of(USER_SCHEMA).withConfiguration(configuration3);

    // Assert that configuration equality check fails
    Assert.assertNotEquals(fileOperations1, SerializableUtils.ensureSerializable(fileOperations3));
  }

  private void writeFile(ResourceId file) throws IOException {
    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA).withCompression(CompressionCodecName.ZSTD);
    final FileOperations.Writer<GenericRecord> writer = fileOperations.createWriter(file);
    for (GenericRecord record : USER_RECORDS) {
      writer.write(record);
    }
    writer.close();
  }
}
