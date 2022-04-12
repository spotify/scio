/*
 * Copyright 2020 Spotify AB.
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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.CoGbkTransform;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroSortedBucketIOTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testReadSerializable() {
    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString())));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString())));
  }

  @Test
  public void testTransformSerializable() {
    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString()))
            .transform(
                AvroSortedBucketIO.transformOutput(String.class, "name", AvroGeneratedUser.class)
                    .to(folder.toString())));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString()))
            .transform(
                AvroSortedBucketIO.transformOutput(
                        String.class, "name", AvroGeneratedUser.getClassSchema())
                    .to(folder.toString())));
  }

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDefaultsTempLocationOpt() {
    final Pipeline pipeline = TestPipeline.create();
    final ResourceId tempDirectory = TestUtils.fromFolder(temporaryFolder);
    pipeline.getOptions().setTempLocation(tempDirectory.toString());

    final SortedBucketIO.Write<String, AvroGeneratedUser> write =
        AvroSortedBucketIO.write(String.class, "name", AvroGeneratedUser.class)
            .to(folder.toString());

    final CoGbkTransform<String, AvroGeneratedUser> transform =
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString()))
            .transform(
                AvroSortedBucketIO.transformOutput(String.class, "name", AvroGeneratedUser.class)
                    .to(folder.toString()));

    Assert.assertEquals(tempDirectory, write.getTempDirectoryOrDefault(pipeline));
    Assert.assertEquals(tempDirectory, transform.getTempDirectoryOrDefault(pipeline));
  }

  @Test
  public void testNumBucketsRequiredParam() {
    Throwable t =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroSortedBucketIO.write(String.class, "name", AvroGeneratedUser.class)
                    .to(folder.toString())
                    .expand(null));
    Assert.assertEquals("numBuckets must be set to a nonzero value", t.getMessage());
  }
}
