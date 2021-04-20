/*
 * Copyright 2019 Spotify AB.
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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.util.MimeTypes;
import org.tensorflow.proto.example.Example;

/**
 * {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for TensorFlow TFRecord
 * files with TensorFlow {@link Example} records.
 */
public class TensorFlowFileOperations extends FileOperations<Example> {

  private TensorFlowFileOperations(Compression compression) {
    super(compression, MimeTypes.BINARY);
  }

  public static TensorFlowFileOperations of(Compression compression) {
    return new TensorFlowFileOperations(compression);
  }

  @Override
  protected Reader<Example> createReader() {
    return new TfReader();
  }

  @Override
  protected FileIO.Sink<Example> createSink() {
    return new FileIO.Sink<Example>() {
      private final TFRecordIO.Sink sink = TFRecordIO.sink();

      @Override
      public void open(WritableByteChannel channel) throws IOException {
        sink.open(channel);
      }

      @Override
      public void write(Example element) throws IOException {
        sink.write(element.toByteArray());
      }

      @Override
      public void flush() throws IOException {
        sink.flush();
      }
    };
  }

  @Override
  public Coder<Example> getCoder() {
    return ProtoCoder.of(Example.class);
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class TfReader extends Reader<Example> {

    private transient TFRecordCodec codec;
    private transient ReadableByteChannel channel;
    private byte[] next;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      this.codec = new TFRecordCodec();
      this.channel = channel;
      next = codec.read(channel);
    }

    @Override
    public Example readNext() throws IOException, NoSuchElementException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      byte[] curr = next;
      next = codec.read(channel);
      return Example.parseFrom(curr);
    }

    @Override
    public boolean hasNextElement() throws IOException {
      return next != null;
    }

    @Override
    public void finishRead() throws IOException {
      channel.close();
    }
  }
}
