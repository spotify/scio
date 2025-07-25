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

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamInputFile implements InputFile {

  private final SeekableByteChannel channel;

  public static InputFile of(
      FileIO.ReadableFile readableFile, SerializableConfiguration configuration)
      throws IOException {
    if (isReadVectorRequested(configuration)) {
      return new VectoredInputFile(readableFile, configuration);
    } else {
      return of(readableFile.getMetadata().resourceId());
    }
  }

  public static InputFile of(java.nio.file.Path path, SerializableConfiguration configuration)
      throws IOException {
    if (isReadVectorRequested(configuration)) {
      return new VectoredInputFile(
          FileSystems.matchNewResource(path.toString(), false),
          FileSystems.matchSingleFileSpec(path.toString()).sizeBytes(),
          configuration);
    } else {
      return of(FileSystems.matchNewResource(path.toString(), false));
    }
  }

  public static BeamInputFile of(SeekableByteChannel seekableByteChannel) {
    return new BeamInputFile(seekableByteChannel);
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

  private static boolean isReadVectorRequested(SerializableConfiguration conf) {
    return conf.get().getBoolean("parquet.hadoop.vectored.io.enabled", false);
  }

  public static class VectoredInputFile implements InputFile, Serializable {
    private final long length;
    private final ResourceId resourceId;
    private final GoogleHadoopFileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(VectoredInputFile.class);

    private VectoredInputFile(
        FileIO.ReadableFile readableFile, SerializableConfiguration configuration)
        throws IOException {
      this(
          readableFile.getMetadata().resourceId(),
          readableFile.getMetadata().sizeBytes(),
          configuration);
    }

    // Todo: detect & warn if gcs-connector < 3.x or java < 17?
    private VectoredInputFile(
        ResourceId resourceId, long sizeBytes, SerializableConfiguration configuration)
        throws IOException {
      this.length = sizeBytes;
      this.resourceId = resourceId;

      // todo handle this better
      if (!resourceId.getScheme().equals("gs")) {
        throw new IllegalArgumentException("Vectored reads are only supported for schemes: [gs]");
      }

      fs = new GoogleHadoopFileSystem();
      final Configuration c = configuration.get();

      // GoogleHadoopFileSystem (gcs-connector) uses a different auth pathway than the FS
      // implementation used by Beam to open ReadableFiles (gcsio's GoogleCloudStorageImpl).
      if (c.get("fs.gs.auth.type") == null) {
        c.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
      }

      fs.initialize(new Path(resourceId.toString()).toUri(), c);
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      // buffer size param is ignored in GFS#open
      final SeekableInputStream stream =
          HadoopStreams.wrap(fs.open(new Path(resourceId.toString()), -1));

      final String className = stream.getClass().getSimpleName();
      if (!className.equals("H1SeekableInputStream")
          && !className.equals("H2SeekableInputStream")) {
        LOG.warn(
            "Vectored read was requested for file {}, but Parquet vector bridge returned non-vectorized stream "
                + "{}. Is parquet-hadoop available on the classpath?",
            resourceId,
            stream.getClass());
      }
      return stream;
    }
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
