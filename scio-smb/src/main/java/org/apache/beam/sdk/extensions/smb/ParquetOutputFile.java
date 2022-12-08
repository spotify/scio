/*
 * Copyright 2021 Spotify AB.
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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class ParquetOutputFile implements OutputFile {
  private final WritableByteChannel channel;

  public ParquetOutputFile(WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new ParquetOutputStream(Channels.newOutputStream(channel));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new ParquetOutputStream(Channels.newOutputStream(channel));
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  private static class ParquetOutputStream extends PositionOutputStream {
    private final OutputStream outputStream;
    private long position = 0;

    private ParquetOutputStream(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      position++;
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }
}
