package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.Coder;

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

public final class IcebergEncoder implements BucketMetadata.Encoder {

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    private byte[] encode(int value) {
        return encode((long) value);
    }

    private byte[] encode(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        return buffer.array();
    }

    private byte[] encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encode(UUID value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
        return buffer.array();
    }

    private byte[] encode(LocalDate value) {
        return encode(ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) value));
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

    private byte[] encode(boolean value) {
        return encode((value) ? 1 : 0);
    }

    private byte[] encode(float value) {
        return encode((double) value);
    }

    private byte[] encode(double value) {
        double canonizedValue = value == -0D ? 0D : value;
        return encode(Double.doubleToLongBits(canonizedValue));
    }

    @Override
    public <T> byte[] encode(T value, Coder<T> coder) {
        if (value instanceof Integer) {
            return encode((Integer) value);
        }
        if (value instanceof Long) {
            return encode((long) value);
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
        if (value instanceof BigDecimal) {
            return encode((BigDecimal) value);
        }

        // types below are not currently valid for bucketing but with a defined hash function
        if (value instanceof Boolean) {
            return encode((Boolean) value);
        }
        if (value instanceof Float) {
            return encode((Float) value);
        }
        if (value instanceof Double) {
            return encode((Double) value);
        }

        throw new UnsupportedOperationException("Unsupported type: " + value.getClass());
    }
}
