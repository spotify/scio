/*
 * Copyright 2024 Spotify AB.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashFunction;
import org.junit.Test;

public class IcebergHashTypeTest {
  @Test
  public void shouldHashValuesAsDescribedInSpec() {
    // https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    BucketMetadata.HashType hashType = BucketMetadata.HashType.ICEBERG;
    HashFunction hasher = hashType.create();
    BucketMetadata.Encoder encoder = hashType.encoder();

    assertEquals(2017239379, hasher.hashBytes(encoder.encode(34, null)).asInt());
    assertEquals(2017239379, hasher.hashBytes(encoder.encode(34L, null)).asInt());
    assertEquals(
        -500754589, hasher.hashBytes(encoder.encode(new BigDecimal("14.20"), null)).asInt());
    assertEquals(
        -653330422, hasher.hashBytes(encoder.encode(LocalDate.of(2017, 11, 16), null)).asInt());
    assertEquals(
        -662762989, hasher.hashBytes(encoder.encode(LocalTime.of(22, 31, 8), null)).asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                encoder.encode(
                    LocalDateTime.of(LocalDate.of(2017, 11, 16), LocalTime.of(22, 31, 8)), null))
            .asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                encoder.encode(
                    ZonedDateTime.of(
                        LocalDate.of(2017, 11, 16),
                        LocalTime.of(14, 31, 8),
                        ZoneOffset.ofHours(-8)),
                    null))
            .asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                encoder.encode(
                    ZonedDateTime.of(
                            LocalDate.of(2017, 11, 16),
                            LocalTime.of(14, 31, 8),
                            ZoneOffset.ofHours(-8))
                        .toInstant(),
                    null))
            .asInt());
    assertEquals(1210000089, hasher.hashBytes(encoder.encode("iceberg", null)).asInt());
    assertEquals(
        1488055340,
        hasher
            .hashBytes(
                encoder.encode(UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"), null))
            .asInt());
    assertEquals(
        -188683207, hasher.hashBytes(encoder.encode(new byte[] {0, 1, 2, 3}, null)).asInt());
  }

  @Test
  public void shouldThrowOnEncodingUnsupportedTypes() {
    // https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    BucketMetadata.HashType hashType = BucketMetadata.HashType.ICEBERG;
    BucketMetadata.Encoder encoder = hashType.encoder();

    assertThrows(UnsupportedOperationException.class, () -> encoder.encode(true, null));
    assertThrows(UnsupportedOperationException.class, () -> encoder.encode(1F, null));
    assertThrows(UnsupportedOperationException.class, () -> encoder.encode(1D, null));
  }
}
