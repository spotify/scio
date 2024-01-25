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
import org.apache.beam.sdk.coders.Coder;

public final class IcebergEncoder implements BucketMetadata.Encoder {

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private byte[] encode(int value) {
    return encode((long) value);
  }

  private byte[] encode(long value) {
    return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
  }

  private byte[] encode(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private byte[] encode(UUID value) {
    return ByteBuffer.allocate(Long.BYTES * 2)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  private byte[] encode(LocalDate value) {
    return encode(ChronoUnit.DAYS.between(EPOCH_DAY, value));
  }

  private byte[] encode(LocalTime value) {
    return encode(value.toNanoOfDay() / 1000);
  }

  private byte[] encode(LocalDateTime value) {
    return encode(value.atOffset(ZoneOffset.UTC).toInstant());
  }

  private byte[] encode(ZonedDateTime value) {
    return encode(value.toInstant());
  }

  private byte[] encode(Instant value) {
    return encode(ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
  }

  private byte[] encode(BigDecimal value) {
    return value.unscaledValue().toByteArray();
  }

  @Override
  public <T> byte[] encode(T value, Coder<T> coder) {
    if (value instanceof Integer) {
      return encode((Integer) value);
    }
    if (value instanceof Long) {
      return encode((long) value);
    }
    if (value instanceof BigDecimal) {
      return encode((BigDecimal) value);
    }
    if (value instanceof CharSequence) {
      return encode((String) value);
    }
    if (value instanceof UUID) {
      return encode((UUID) value);
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof LocalDate) {
      return encode((LocalDate) value);
    }
    if (value instanceof LocalTime) {
      return encode((LocalTime) value);
    }
    if (value instanceof LocalDateTime) {
      return encode((LocalDateTime) value);
    }
    if (value instanceof ZonedDateTime) {
      return encode((ZonedDateTime) value);
    }
    if (value instanceof Instant) {
      return encode((Instant) value);
    }

    throw new UnsupportedOperationException("Unsupported type: " + value.getClass());
  }
}
