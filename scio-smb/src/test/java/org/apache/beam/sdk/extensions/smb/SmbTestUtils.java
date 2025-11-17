/*
 * Copyright 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

/**
 * Test utilities for SMB operations. Must be in same package as BucketMetadata to access
 * package-private methods.
 */
public class SmbTestUtils {

  /**
   * Hash a key to a bucket ID using BucketMetadata's hashing.
   *
   * @param metadata metadata containing hash configuration
   * @param key the key to hash
   * @return bucket ID (0 to numBuckets-1)
   */
  public static <K> int hashToBucket(BucketMetadata<K, ?, ?> metadata, K key) {
    byte[] keyBytes = metadata.encodeKeyBytesPrimary(key);
    return metadata.getBucketId(keyBytes);
  }

  /**
   * Read all records from an Avro file using raw Avro DataFileReader.
   *
   * <p>Uses GenericDatumReader to read the file, which works for both GenericRecord and specific
   * record types (specific records are GenericRecords internally).
   *
   * @param metadata metadata (currently unused but kept for API compatibility)
   * @param file file to read
   * @param <T> record type
   * @return list of records
   */
  @SuppressWarnings("unused")
  public static <T> List<T> readAvroFile(BucketMetadata<?, ?, T> metadata, File file)
      throws IOException {
    List<T> records = new ArrayList<>();

    // Use GenericDatumReader - works for both GenericRecord and specific record types
    try (DataFileReader<T> reader = new DataFileReader<>(file, new GenericDatumReader<>())) {
      while (reader.hasNext()) {
        records.add(reader.next());
      }
    }

    return records;
  }
}
