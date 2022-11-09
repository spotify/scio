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

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link BucketMetadataUtil}. */
public class BucketMetadataUtilTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final BucketMetadataUtil util = new BucketMetadataUtil(2);

  @Test
  public void testIncompatibleMetadata() throws Exception {
    final List<TestBucketMetadata> metadataList1 =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  try {
                    return TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1)
                        .withKeyIndex(i != 9 ? 0 : 1);
                  } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    testIncompatibleMetadata(metadataList1, 0, 9);

    Collections.reverse(metadataList1);
    testIncompatibleMetadata(metadataList1, 9, 0);

    final List<TestBucketMetadata> metadataList2 =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  try {
                    return TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1)
                        .withKeyIndex(i != 4 ? 0 : 1);
                  } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    testIncompatibleMetadata(metadataList2, 0, 4);

    Collections.reverse(metadataList2);
    testIncompatibleMetadata(metadataList2, 9, 5);
  }

  private void testIncompatibleMetadata(
      List<TestBucketMetadata> metadataList, int canonicalIdx, int badIdx) throws Exception {
    final List<String> directories = new ArrayList<>();
    final List<String> goodDirectories = new ArrayList<>();

    // all but one metadata are source-compatible, the one at badIdx is incompatible
    for (int i = 0; i < metadataList.size(); i++) {
      final File dest = folder.newFolder(String.valueOf(i));
      final OutputStream outputStream =
          Channels.newOutputStream(
              FileSystems.create(
                  LocalResources.fromFile(folder.newFile(i + "/metadata.json"), false),
                  "application/json"));

      BucketMetadata.to(metadataList.get(i), outputStream);
      directories.add(dest.getAbsolutePath());
      if (i != badIdx) {
        goodDirectories.add(dest.getAbsolutePath());
      }
    }

    final TestBucketMetadata canonicalMetadata = metadataList.get(canonicalIdx);

    final SourceMetadata<String, String> sourceMetadata =
        util.getSourceMetadata(goodDirectories, ".txt");
    Assert.assertEquals(canonicalMetadata, sourceMetadata.getCanonicalMetadata());
    Assert.assertEquals(goodDirectories.size(), sourceMetadata.getPartitionMetadata().size());

    Assert.assertThrows(
        IllegalStateException.class, () -> util.getSourceMetadata(directories, ".txt"));

    folder.delete();
  }

  @Test
  public void testMissingMetadata() throws Exception {
    final List<Optional<TestBucketMetadata>> metadataList =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  if (i == 9) {
                    return Optional.<TestBucketMetadata>empty();
                  } else {
                    try {
                      return Optional.of(TestBucketMetadata.of((int) Math.pow(2.0, 1.0 * i), 1));
                    } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
                      throw new RuntimeException(e);
                    }
                  }
                })
            .collect(Collectors.toList());

    testMissingMetadata(metadataList);

    Collections.reverse(metadataList);
    testMissingMetadata(metadataList);
  }

  private void testMissingMetadata(List<Optional<TestBucketMetadata>> metadataList)
      throws Exception {
    final List<String> directories = new ArrayList<>();

    // all but one metadata are compatible
    ResourceId missingMetadataDir = null;
    for (int i = 0; i < metadataList.size(); i++) {
      final File dest = folder.newFolder(String.valueOf(i));
      directories.add(dest.getAbsolutePath());

      if (!metadataList.get(i).isPresent()) {
        missingMetadataDir = LocalResources.fromFile(dest, true);
        continue;
      }

      final TestBucketMetadata metadata = metadataList.get(i).get();
      final OutputStream outputStream =
          Channels.newOutputStream(
              FileSystems.create(
                  LocalResources.fromFile(folder.newFile(i + "/metadata.json"), false),
                  "application/json"));

      BucketMetadata.to(metadata, outputStream);
    }

    Assert.assertThrows(
        "Could not find SMB metadata for source directory " + missingMetadataDir,
        RuntimeException.class,
        () -> util.getSourceMetadata(directories, ".txt"));

    folder.delete();
  }
}
