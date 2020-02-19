/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.parquet;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public class BeamInputFile implements InputFile {

  private final SeekableByteChannel channel;

  public static BeamInputFile of(SeekableByteChannel channel) {
    return new BeamInputFile(channel);
  }

  public static BeamInputFile of(ResourceId resourceId) throws IOException {
    return of((SeekableByteChannel) FileSystems.open(resourceId));
  }

  public static BeamInputFile of(String path) throws IOException {
    return of(FileSystems.matchSingleFileSpec(path).resourceId());
  }

  private BeamInputFile(SeekableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public long getLength() throws IOException {
    return channel.size();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new BeamInputStream(channel);
  }

  private static class BeamInputStream extends DelegatingSeekableInputStream {
    private final SeekableByteChannel channel;

    private BeamInputStream(SeekableByteChannel channel) {
      super(Channels.newInputStream(channel));
      this.channel = channel;
    }

    @Override
    public long getPos() throws IOException {
      return channel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
      channel.position(newPos);
    }
  }
}
