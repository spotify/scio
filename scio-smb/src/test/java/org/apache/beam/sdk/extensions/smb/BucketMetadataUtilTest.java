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

import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link BucketMetadataUtil}. */
public class BucketMetadataUtilTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final BucketMetadataUtil util = new BucketMetadataUtil(2);

  @Test
  public void testIncompatibleMetadata() throws Exception {
    final List<ResourceId> directories = new ArrayList<>();
    final List<TestBucketMetadata> metadataList = new ArrayList<>();

    // first 9 elements are source-compatible, last is not
    for (int i = 0; i < 10; i++) {
      final File dest = folder.newFolder(String.valueOf(i));
      final TestBucketMetadata metadata =
          TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1).withKeyIndex(i < 9 ? 0 : 1);
      final OutputStream outputStream =
          Channels.newOutputStream(
              FileSystems.create(
                  LocalResources.fromFile(folder.newFile(i + "/metadata.json"), false),
                  "application/json"));

      BucketMetadata.to(metadata, outputStream);
      directories.add(LocalResources.fromFile(dest, true));
      metadataList.add(metadata);
    }

    final TestBucketMetadata canonicalMetadata = metadataList.get(0);

    final SourceMetadata<String, String> sourceMetadata =
        util.getSourceMetadata(directories.subList(0, 9), ".txt");
    Assert.assertEquals(canonicalMetadata, sourceMetadata.getCanonicalMetadata());
    Assert.assertEquals(9, sourceMetadata.getPartitionMetadata().size());

    Assert.assertThrows(IllegalStateException.class, () -> util.getSourceMetadata(directories, ".txt"));

    Collections.reverse(directories);
    Assert.assertThrows(IllegalStateException.class, () -> util.getSourceMetadata(directories, ".txt"));

    Collections.shuffle(directories);
    Assert.assertThrows(IllegalStateException.class, () -> util.getSourceMetadata(directories, ".txt"));
  }

  @Test
  public void testMissingMetadata() throws Exception {
    final List<ResourceId> directories = new ArrayList<>();

    // first 9 elements are source-compatible, last is missing metadata.json
    for (int i = 0; i < 10; i++) {
      final File dest = folder.newFolder(String.valueOf(i));
      directories.add(LocalResources.fromFile(dest, true));
      if (i == 9) {
        break;
      }

      final TestBucketMetadata metadata =
          TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1);
      final OutputStream outputStream =
          Channels.newOutputStream(
              FileSystems.create(
                  LocalResources.fromFile(folder.newFile(i + "/metadata.json"), false),
                  "application/json"));

      BucketMetadata.to(metadata, outputStream);
    }

    Assert.assertFalse(util.getSourceMetadata(directories, ".txt").supportsSmb());

    Collections.reverse(directories);
    Assert.assertFalse(util.getSourceMetadata(directories, ".txt").supportsSmb());

    Collections.shuffle(directories);
    Assert.assertFalse(util.getSourceMetadata(directories, ".txt").supportsSmb());
  }
}
