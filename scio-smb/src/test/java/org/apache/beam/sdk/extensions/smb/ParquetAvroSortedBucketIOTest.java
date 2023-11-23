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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParquetAvroSortedBucketIOTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testReadSerializable() {
    final Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(
        conf,
        Schema.createRecord(
            Lists.newArrayList(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), "", ""))));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                ParquetAvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString())
                    .withConfiguration(conf)
                    .withFilterPredicate(FilterApi.lt(FilterApi.intColumn("test"), 5))));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                ParquetAvroSortedBucketIO.read(
                        new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString())
                    .withConfiguration(conf)
                    .withFilterPredicate(FilterApi.lt(FilterApi.intColumn("test"), 5))));
  }

  @Test
  public void testTransformSerializable() {
    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                ParquetAvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString()))
            .transform(
                ParquetAvroSortedBucketIO.transformOutput(
                        String.class, "name", AvroGeneratedUser.class)
                    .to(folder.toString())));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                ParquetAvroSortedBucketIO.read(
                        new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString()))
            .transform(
                ParquetAvroSortedBucketIO.transformOutput(
                        String.class, "name", AvroGeneratedUser.getClassSchema())
                    .to(folder.toString())));
  }

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDefaultsTempLocationOpt() {
    final Pipeline pipeline = TestPipeline.create();
    final ResourceId tempDirectory = TestUtils.fromFolder(temporaryFolder);
    pipeline.getOptions().setTempLocation(tempDirectory.toString());

    final SortedBucketIO.Write<String, Void, GenericRecord> write =
        ParquetAvroSortedBucketIO.write(String.class, "name", AvroGeneratedUser.getClassSchema())
            .to(folder.toString());

    final SortedBucketIO.CoGbkTransform<String, GenericRecord> transform =
        SortedBucketIO.read(String.class)
            .of(
                ParquetAvroSortedBucketIO.read(
                        new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString()))
            .transform(
                ParquetAvroSortedBucketIO.transformOutput(
                        String.class, "name", AvroGeneratedUser.getClassSchema())
                    .to(folder.toString()));

    Assert.assertEquals(tempDirectory, write.getTempDirectoryOrDefault(pipeline));
    Assert.assertEquals(tempDirectory, transform.getTempDirectoryOrDefault(pipeline));
  }
}
