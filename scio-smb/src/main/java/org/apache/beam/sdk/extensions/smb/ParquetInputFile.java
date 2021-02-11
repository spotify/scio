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

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

public class ParquetInputFile implements InputFile {
  private final SeekableByteChannel channel;

  public ParquetInputFile(ReadableByteChannel channel) {
    this.channel = (SeekableByteChannel) channel;
  }

  @Override
  public long getLength() throws IOException {
    return channel.size();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {

      @Override
      public long getPos() throws IOException {
        return channel.position();
      }

      @Override
      public void seek(long newPos) throws IOException {
        channel.position(newPos);
      }
    };
  }
}
