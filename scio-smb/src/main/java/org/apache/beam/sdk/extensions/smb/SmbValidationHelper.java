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

import java.util.List;

/**
 * Internal helper for SMB validation operations in Scio SMBCollection.
 *
 * <p>This class provides bridge methods to access package-private Beam SMB validation logic from
 * the Scala SMBCollection implementation.
 *
 * <p><b>DO NOT USE - This is an internal implementation detail subject to change without
 * notice.</b>
 *
 * @hidden
 */
@org.apache.beam.sdk.annotations.Internal
public class SmbValidationHelper {

  /**
   * Validate that SMB sources are compatible.
   *
   * <p>This validates: - Hash type compatibility (all sources must use same hash type) - Bucket
   * count compatibility (must be powers of 2) - Key class compatibility
   *
   * @param sources list of bucketed inputs to validate
   * @throws IllegalStateException if sources are incompatible
   */
  public static void validateSources(List<SortedBucketSource.BucketedInput<?>> sources) {
    // SourceSpec.from() reads metadata and validates compatibility
    // It throws IllegalStateException if sources are incompatible
    SourceSpec.from(sources);
  }

  /**
   * Get metadata from the first source.
   *
   * @param sources list of bucketed inputs
   * @return metadata from first source, or null if no sources or no metadata
   */
  public static BucketMetadata<?, ?, ?> getFirstMetadata(
      List<SortedBucketSource.BucketedInput<?>> sources) {
    if (sources.isEmpty()) {
      return null;
    }

    SortedBucketSource.BucketedInput<?> firstInput = sources.get(0);
    return firstInput.getSourceMetadata().mapping.values().stream()
        .findFirst()
        .map(metadataAndBuckets -> metadataAndBuckets.metadata)
        .orElse(null);
  }

  /**
   * Check if metadata's key class matches the expected type.
   *
   * @param metadata the bucket metadata to check
   * @param expectedKeyClass the expected key class
   * @return true if key classes match
   */
  public static <K> boolean keyClassMatches(
      BucketMetadata<?, ?, ?> metadata, Class<K> expectedKeyClass) {
    return metadata.keyClassMatches(expectedKeyClass);
  }

  /**
   * Check if metadata's secondary key class matches the expected type.
   *
   * <p>For backward compatibility: when expecting Void.class (primary-only keys), we accept sources
   * with either Void.class or null secondary key class.
   *
   * @param metadata the bucket metadata to check
   * @param expectedSecondaryKeyClass the expected secondary key class
   * @return true if secondary key classes match
   */
  public static <K> boolean keyClassSecondaryMatches(
      BucketMetadata<?, ?, ?> metadata, Class<K> expectedSecondaryKeyClass) {
    Class<?> actualSecondary = metadata.getKeyClassSecondary();

    // For primary-only keys (Void.class), accept both null and Void.class
    // This maintains backward compatibility with sources written without secondary keys
    if (expectedSecondaryKeyClass.equals(Void.class)) {
      return actualSecondary == null || actualSecondary.equals(Void.class);
    }

    // For composite keys, require exact match
    return actualSecondary != null && actualSecondary.equals(expectedSecondaryKeyClass);
  }
}
