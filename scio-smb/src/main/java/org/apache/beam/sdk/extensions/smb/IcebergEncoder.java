package org.apache.beam.sdk.extensions.smb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.beam.sdk.coders.Coder;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public final class IcebergEncoder implements BucketMetadata.Encoder {

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    private byte[] encode(int value) {
        return encode((long) value);
    }

    private byte[] encode(long value) {
        Slice slice = Slices.allocate(Long.BYTES);
        slice.setLong(0, value);
        return slice.getBytes();
    }

    private byte[] encode(String value) {
        return Slices.utf8Slice(value).getBytes();
    }

    private byte[] encode(UUID value) {
        Slice slice = Slices.allocate(Long.BYTES * 2);
        slice.setLong(0, Long.reverseBytes(value.getMostSignificantBits()));
        slice.setLong(Long.BYTES, Long.reverseBytes(value.getLeastSignificantBits()));
        return slice.getBytes();
    }

    private byte[] encode(LocalDate value) {
        return encode(ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) value));
    }

    private byte[] encode(LocalTime value) {
        return encode(value.toNanoOfDay() / 1000);
    }

    private byte[] encode(Instant value) {
        return encode(ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
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
        if (value instanceof Instant) {
            return encode((Instant) value);
        }

        throw new UnsupportedOperationException("Unsupported type: " + value.getClass());
    }
}
