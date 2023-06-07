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

import com.spotify.scio.smb.TestLogicalTypes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroDataSupplier;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroWriteSupport;
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
      Schema.createRecord(
          "User",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("name", Schema.create(Schema.Type.STRING), "", ""),
              new Schema.Field("age", Schema.create(Schema.Type.INT), "", 0)));

  private static final List<GenericRecord> USER_RECORDS =
      IntStream.range(0, 10)
          .mapToObj(
              i ->
                  new GenericRecordBuilder(USER_SCHEMA)
                      .set("name", String.format("user%02d", i))
                      .set("age", i)
                      .build())
          .collect(Collectors.toList());

  @Test
  public void testGenericRecord() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    writeFile(file);

    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA);

    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(USER_RECORDS, actual);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final ParquetAvroFileOperations<AvroGeneratedUser> fileOperations =
        ParquetAvroFileOperations.of(AvroGeneratedUser.getClassSchema());
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
    conf.setClass(
        AvroWriteSupport.AVRO_DATA_SUPPLIER, AvroLogicalTypeSupplier.class, AvroDataSupplier.class);
    conf.setClass(
        AvroReadSupport.AVRO_DATA_SUPPLIER, AvroLogicalTypeSupplier.class, AvroDataSupplier.class);

    final ParquetAvroFileOperations<TestLogicalTypes> fileOperations =
        ParquetAvroFileOperations.of(
            TestLogicalTypes.getClassSchema(), CompressionCodecName.UNCOMPRESSED, conf);
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
  public void testProjection() throws Exception {
    final ResourceId file =
        fromFolder(output)
            .resolve("file.parquet", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    writeFile(file);

    final Schema projection =
        Schema.createRecord(
            "UserProjection",
            "",
            "org.apache.beam.sdk.extensions.smb.avro",
            false,
            Lists.newArrayList(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), "", "")));
    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(projection);

    final List<GenericRecord> expected =
        USER_RECORDS.stream()
            .map(r -> new GenericRecordBuilder(projection).set("name", r.get("name")).build())
            .collect(Collectors.toList());
    final List<GenericRecord> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

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
        ParquetAvroFileOperations.of(USER_SCHEMA, predicate);

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

  private void writeFile(ResourceId file) throws IOException {
    final ParquetAvroFileOperations<GenericRecord> fileOperations =
        ParquetAvroFileOperations.of(USER_SCHEMA, CompressionCodecName.ZSTD);
    final FileOperations.Writer<GenericRecord> writer = fileOperations.createWriter(file);
    for (GenericRecord record : USER_RECORDS) {
      writer.write(record);
    }
    writer.close();
  }
}
