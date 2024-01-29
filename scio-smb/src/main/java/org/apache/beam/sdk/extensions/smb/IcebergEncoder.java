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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public final class IcebergEncoder {

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

  private IcebergEncoder() {}

  private static byte[] encode(int value) {
    return encode((long) value);
  }

  private static byte[] encode(long value) {
    return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
  }

  private static byte[] encode(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] encode(UUID value) {
    return ByteBuffer.allocate(Long.BYTES * 2)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  private static byte[] encode(LocalDate value) {
    return encode(value.toEpochDay());
  }

  private static byte[] encode(LocalTime value) {
    return encode(value.toNanoOfDay() / 1000);
  }

  private static byte[] encode(LocalDateTime value) {
    return encode(value.atOffset(ZoneOffset.UTC).toInstant());
  }

  private static byte[] encode(ZonedDateTime value) {
    return encode(value.toInstant());
  }

  private static byte[] encode(Instant value) {
    return encode(ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
  }

  private static byte[] encode(BigDecimal value) {
    return value.unscaledValue().toByteArray();
  }

  public static <T> BucketMetadata.KeyEncoder<T> create(Class<? super T> klass) {
    if (klass.isAssignableFrom(Integer.class)) {
      return value -> encode((Integer) value);
    }
    if (klass.isAssignableFrom(Long.class)) {
      return value -> encode((long) value);
    }
    if (klass.isAssignableFrom(BigDecimal.class)) {
      return value -> encode((BigDecimal) value);
    }
    if (klass.isAssignableFrom(String.class)) {
      return value -> encode((String) value);
    }
    if (klass.isAssignableFrom(UUID.class)) {
      return value -> encode((UUID) value);
    }
    if (klass.isAssignableFrom(byte[].class)) {
      return value -> (byte[]) value;
    }
    if (klass.isAssignableFrom(LocalDate.class)) {
      return value -> encode((LocalDate) value);
    }
    if (klass.isAssignableFrom(LocalTime.class)) {
      return value -> encode((LocalTime) value);
    }
    if (klass.isAssignableFrom(LocalDateTime.class)) {
      return value -> encode((LocalDateTime) value);
    }
    if (klass.isAssignableFrom(ZonedDateTime.class)) {
      return value -> encode((ZonedDateTime) value);
    }
    if (klass.isAssignableFrom(Instant.class)) {
      return value -> encode((Instant) value);
    }

    throw new UnsupportedOperationException("Unsupported type: " + klass);
  }
}
