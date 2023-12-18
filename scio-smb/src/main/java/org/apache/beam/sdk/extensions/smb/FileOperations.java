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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.PatchedReadableFileUtil;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts IO operations for file-based formats.
 *
 * <p>Since the SMB algorithm doesn't support {@link org.apache.beam.sdk.io.Source} splitting, I/O
 * operations must be abstracted at a per-record granularity. {@link Reader} and {@link Writer} must
 * be {@link Serializable} to be used in {@link SortedBucketSource} and {@link SortedBucketSink}
 * transforms.
 */
public abstract class FileOperations<V> implements Serializable, HasDisplayData {

  private static final Logger LOG = LoggerFactory.getLogger(FileOperations.class);
  private static final AtomicReference<Long> diskBufferBytes = new AtomicReference<>(null);

  private static final Counter filesStreamed =
      Metrics.counter(FileOperations.class, "SortedBucketSource-FilesStreamed");
  private static final Counter filesBuffered =
      Metrics.counter(FileOperations.class, "SortedBucketSource-FilesBuffered");
  private static final Counter bytesBuffered =
      Metrics.counter(FileOperations.class, "SortedBucketSource-BytesBuffered");

  private final Compression compression;
  private final String mimeType;

  public static void setDiskBufferMb(int diskBufferMb) {
    diskBufferBytes.compareAndSet(null, diskBufferMb * 1024L * 1024L);
  }

  protected FileOperations(Compression compression, String mimeType) {
    this.compression = compression;
    this.mimeType = mimeType;
  }

  protected abstract Reader<V> createReader();

  // Delegate to FileIO.Sink<V> for writer logic
  protected abstract FileIO.Sink<V> createSink();

  public abstract Coder<V> getCoder();

  public final Iterator<V> iterator(ResourceId resourceId) throws IOException {
    final ReadableFile readableFile = toReadableFile(resourceId);

    final Reader<V> reader = createReader();

    Long bytes = diskBufferBytes.get();
    if (bytes != null && bytes > 0) {
      final long fileSize = readableFile.getMetadata().sizeBytes();
      final long prevSize = diskBufferBytes.getAndUpdate(prev -> prev > 0 ? prev - fileSize : prev);

      // Buffer available, an update was made
      if (prevSize > 0) {
        LOG.debug("Buffering SMB source file {}, size = {}B", resourceId, fileSize);
        String tmpDir = System.getProperties().getProperty("java.io.tmpdir");
        Path path = Paths.get(tmpDir, String.format("smb-buffer-%s", UUID.randomUUID()));
        ReadableByteChannel src = readableFile.open();
        FileChannel dst =
            FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        long copied = 0;
        do {
          copied += dst.transferFrom(src, copied, fileSize - copied);
        } while (copied < fileSize);
        dst.close();
        src.close();

        bytesBuffered.inc(fileSize);
        filesBuffered.inc();
        reader.whenDone(
            () -> {
              path.toFile().delete();
              return diskBufferBytes.getAndUpdate(prev -> prev + fileSize);
            });
        reader.prepareRead(Files.newByteChannel(path));
        return reader.iterator();
      }
    }

    filesStreamed.inc();
    reader.prepareRead(readableFile.open());
    return reader.iterator();
  }

  public Writer<V> createWriter(ResourceId resourceId) throws IOException {
    final Writer<V> writer = new Writer<>(createSink(), compression);
    writer.prepareWrite(FileSystems.create(resourceId, mimeType));
    return writer;
  }

  @Override
  public void populateDisplayData(Builder builder) {
    builder.add(DisplayData.item("FileOperations", getClass()));
    builder.add(DisplayData.item("compression", compression.toString()));
    builder.add(DisplayData.item("mimeType", mimeType));
  }

  /** Per-element file reader. */
  public abstract static class Reader<V> implements Serializable {
    private transient Supplier<?> cleanupFn = null;

    private void whenDone(Supplier<?> cleanupFn) {
      this.cleanupFn = cleanupFn;
    }

    public abstract void prepareRead(ReadableByteChannel channel) throws IOException;

    /** Reads next record in the collection. */
    public abstract V readNext() throws IOException, NoSuchElementException;

    public abstract boolean hasNextElement() throws IOException;

    public abstract void finishRead() throws IOException;

    Iterator<V> iterator() {
      return new Iterator<V>() {
        private boolean finished = false;

        @Override
        public boolean hasNext() {
          if (finished) {
            return false;
          }

          try {
            boolean hasNext = hasNextElement();

            if (!hasNext) {
              finishRead();
              if (cleanupFn != null) {
                cleanupFn.get();
              }
              finished = true;
            }

            return hasNext;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public V next() {
          if (finished) {
            throw new NoSuchElementException();
          }

          try {
            return readNext();
          } catch (IOException e) {
            finished = true;
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  /** Per-element file writer. */
  public static class Writer<V> implements Serializable, AutoCloseable {

    private final FileIO.Sink<V> sink;
    private transient WritableByteChannel channel;
    private Compression compression;

    Writer(FileIO.Sink<V> sink, Compression compression) {
      this.sink = sink;
      this.compression = compression;
    }

    private void prepareWrite(WritableByteChannel channel) throws IOException {
      this.channel = compression.writeCompressed(channel);
      sink.open(this.channel);
    }

    public void write(V value) throws IOException {
      sink.write(value);
    }

    @Override
    public void close() throws IOException {
      try {
        sink.flush();
      } catch (IOException e) {
        // always close channel
        channel.close();
        throw e;
      }
      channel.close();
    }
  }

  private ReadableFile toReadableFile(ResourceId resourceId) {
    try {
      final Metadata metadata = FileSystems.matchSingleFileSpec(resourceId.toString());

      return PatchedReadableFileUtil.newReadableFile(
          metadata,
          compression == Compression.AUTO
              ? Compression.detect(resourceId.getFilename())
              : compression);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Exception opening bucket file %s", resourceId), e);
    }
  }

  static class FileOperationsCoder<T> extends CustomCoder<FileOperations<T>> {
    @Override
    public void encode(FileOperations<T> value, OutputStream outStream)
        throws CoderException, IOException {
      final ObjectOutputStream oos = new ObjectOutputStream(outStream);
      oos.writeObject(value);
      oos.flush();
    }

    @Override
    public FileOperations<T> decode(InputStream inStream) throws CoderException, IOException {
      final ObjectInputStream ois = new ObjectInputStream(inStream);
      try {
        return (FileOperations<T>) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new CoderException(e);
      }
    }
  }
}
