/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.spotify.scio.smb.annotations.PatchedFromBeam;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

/**
 * Codec for TFRecords file format. See
 * https://www.tensorflow.org/api_guides/python/python_io#TFRecords_Format_Details
 */
@PatchedFromBeam(origin="org.apache.beam.sdk.io.TFRecordIO")
public class TFRecordCodec {
  private static final int HEADER_LEN = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
  private static final int FOOTER_LEN = Integer.SIZE / Byte.SIZE;
  private static HashFunction crc32c = Hashing.crc32c();

  private ByteBuffer header = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.LITTLE_ENDIAN);
  private ByteBuffer footer = ByteBuffer.allocate(FOOTER_LEN).order(ByteOrder.LITTLE_ENDIAN);

  private int mask(int crc) {
    return ((crc >>> 15) | (crc << 17)) + 0xa282ead8;
  }

  private int hashLong(long x) {
    return mask(crc32c.hashLong(x).asInt());
  }

  private int hashBytes(byte[] x) {
    return mask(crc32c.hashBytes(x).asInt());
  }

  public int recordLength(byte[] data) {
    return HEADER_LEN + data.length + FOOTER_LEN;
  }

  @Nullable
  byte[] read(ReadableByteChannel inChannel) throws IOException {
    header.clear();
    int headerBytes = inChannel.read(header);
    if (headerBytes <= 0) {
      return null;
    }
    checkState(headerBytes == HEADER_LEN, "Not a valid TFRecord. Fewer than 12 bytes.");

    header.rewind();
    long length = header.getLong();
    long lengthHash = hashLong(length);
    int maskedCrc32OfLength = header.getInt();
    if (lengthHash != maskedCrc32OfLength) {
      throw new IOException(
          String.format(
              "Mismatch of length mask when reading a record. Expected %d but received %d.",
              maskedCrc32OfLength, lengthHash));
    }

    ByteBuffer data = ByteBuffer.allocate((int) length);
    while (data.hasRemaining() && inChannel.read(data) >= 0) {}
    if (data.hasRemaining()) {
      throw new IOException(
          String.format(
              "EOF while reading record of length %d. Read only %d bytes. Input might be truncated.",
              length, data.position()));
    }

    footer.clear();
    inChannel.read(footer);
    footer.rewind();

    int maskedCrc32OfData = footer.getInt();
    int dataHash = hashBytes(data.array());
    if (dataHash != maskedCrc32OfData) {
      throw new IOException(
          String.format(
              "Mismatch of data mask when reading a record. Expected %d but received %d.",
              maskedCrc32OfData, dataHash));
    }
    return data.array();
  }

  public void write(WritableByteChannel outChannel, byte[] data) throws IOException {
    int maskedCrc32OfLength = hashLong(data.length);
    int maskedCrc32OfData = hashBytes(data);

    header.clear();
    header.putLong(data.length).putInt(maskedCrc32OfLength);
    header.rewind();
    outChannel.write(header);

    outChannel.write(ByteBuffer.wrap(data));

    footer.clear();
    footer.putInt(maskedCrc32OfData);
    footer.rewind();
    outChannel.write(footer);
  }
}