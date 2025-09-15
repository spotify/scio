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
import java.time.Instant;
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
    BucketMetadata.KeyEncoder<Integer> integerKeyEncoder = IcebergEncoder.create(Integer.class);
    BucketMetadata.KeyEncoder<Long> longKeyEncoder = IcebergEncoder.create(Long.class);
    BucketMetadata.KeyEncoder<BigDecimal> decimalKeyEncoder =
        IcebergEncoder.create(BigDecimal.class);
    BucketMetadata.KeyEncoder<LocalDate> dateKeyEncoder = IcebergEncoder.create(LocalDate.class);
    BucketMetadata.KeyEncoder<LocalTime> timeKeyEncoder = IcebergEncoder.create(LocalTime.class);
    BucketMetadata.KeyEncoder<LocalDateTime> dateTimeKeyEncoder =
        IcebergEncoder.create(LocalDateTime.class);
    BucketMetadata.KeyEncoder<ZonedDateTime> zonedDateTimeKeyEncoder =
        IcebergEncoder.create(ZonedDateTime.class);
    BucketMetadata.KeyEncoder<Instant> instantKeyEncoder = IcebergEncoder.create(Instant.class);
    BucketMetadata.KeyEncoder<String> stringKeyEncoder = IcebergEncoder.create(String.class);
    BucketMetadata.KeyEncoder<UUID> uuidKeyEncoder = IcebergEncoder.create(UUID.class);
    BucketMetadata.KeyEncoder<byte[]> bytesKeyEncoder = IcebergEncoder.create(byte[].class);

    assertEquals(2017239379, hasher.hashBytes(integerKeyEncoder.encode(34)).asInt());
    assertEquals(2017239379, hasher.hashBytes(longKeyEncoder.encode(34L)).asInt());
    assertEquals(
        -500754589, hasher.hashBytes(decimalKeyEncoder.encode(new BigDecimal("14.20"))).asInt());
    assertEquals(
        -653330422, hasher.hashBytes(dateKeyEncoder.encode(LocalDate.of(2017, 11, 16))).asInt());
    assertEquals(
        -662762989, hasher.hashBytes(timeKeyEncoder.encode(LocalTime.of(22, 31, 8))).asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                dateTimeKeyEncoder.encode(
                    LocalDateTime.of(LocalDate.of(2017, 11, 16), LocalTime.of(22, 31, 8))))
            .asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                zonedDateTimeKeyEncoder.encode(
                    ZonedDateTime.of(
                        LocalDate.of(2017, 11, 16),
                        LocalTime.of(14, 31, 8),
                        ZoneOffset.ofHours(-8))))
            .asInt());
    assertEquals(
        -2047944441,
        hasher
            .hashBytes(
                instantKeyEncoder.encode(
                    ZonedDateTime.of(
                            LocalDate.of(2017, 11, 16),
                            LocalTime.of(14, 31, 8),
                            ZoneOffset.ofHours(-8))
                        .toInstant()))
            .asInt());
    assertEquals(1210000089, hasher.hashBytes(stringKeyEncoder.encode("iceberg")).asInt());
    assertEquals(
        1488055340,
        hasher
            .hashBytes(
                uuidKeyEncoder.encode(UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7")))
            .asInt());
    assertEquals(
        -188683207, hasher.hashBytes(bytesKeyEncoder.encode(new byte[] {0, 1, 2, 3})).asInt());
  }

  @Test
  public void shouldThrowOnEncodingUnsupportedTypes() {
    // https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    BucketMetadata.HashType hashType = BucketMetadata.HashType.ICEBERG;

    assertThrows(UnsupportedOperationException.class, () -> IcebergEncoder.create(Boolean.class));
    assertThrows(UnsupportedOperationException.class, () -> IcebergEncoder.create(Float.class));
    assertThrows(UnsupportedOperationException.class, () -> IcebergEncoder.create(Double.class));
  }
}
