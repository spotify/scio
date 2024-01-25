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

public final class IcebergEncoder<T> {

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

  public static <T> BucketMetadata.Encoder<T> create(Class<T> klass) {
    if (klass.equals(Integer.class)) {
      return (value, coder) -> encode((Integer) value);
    }
    if (klass.equals(Long.class)) {
      return (value, coder) -> encode((long) value);
    }
    if (klass.equals(BigDecimal.class)) {
      return (value, coder) -> encode((BigDecimal) value);
    }
    if (klass.equals(CharSequence.class) || klass.equals(String.class)) {
      return (value, coder) -> encode((String) value);
    }
    if (klass.equals(UUID.class)) {
      return (value, coder) -> encode((UUID) value);
    }
    if (klass.equals(byte[].class)) {
      return (value, coder) -> (byte[]) value;
    }
    if (klass.equals(LocalDate.class)) {
      return (value, coder) -> encode((LocalDate) value);
    }
    if (klass.equals(LocalTime.class)) {
      return (value, coder) -> encode((LocalTime) value);
    }
    if (klass.equals(LocalDateTime.class)) {
      return (value, coder) -> encode((LocalDateTime) value);
    }
    if (klass.equals(ZonedDateTime.class)) {
      return (value, coder) -> encode((ZonedDateTime) value);
    }
    if (klass.equals(Instant.class)) {
      return (value, coder) -> encode((Instant) value);
    }

    throw new UnsupportedOperationException("Unsupported type: " + klass);
  }
}
