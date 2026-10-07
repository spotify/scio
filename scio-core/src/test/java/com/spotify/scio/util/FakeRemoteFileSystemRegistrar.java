/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Registers an in-memory, read-only {@link FileSystem} under the {@code fakeremote://} scheme so
 * that {@link RemoteFileUtil} can be exercised against a "remote" source whose channel misbehaves
 * (ends early or fails mid-read).
 */
public class FakeRemoteFileSystemRegistrar implements FileSystemRegistrar {

  public static final String SCHEME = "fakeremote";

  /** A fake remote object. */
  public static final class Entry {
    final long declaredSize;
    final byte[] content;
    final long failAfter;

    /**
     * @param declaredSize size reported by {@code match}
     * @param content bytes actually served by the channel
     * @param failAfter throw an {@link IOException} from {@code read} once this many bytes have
     *     been served, or -1 to never fail
     */
    public Entry(long declaredSize, byte[] content, long failAfter) {
      this.declaredSize = declaredSize;
      this.content = content;
      this.failAfter = failAfter;
    }
  }

  private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();
  private static final AtomicInteger OPEN_CHANNELS = new AtomicInteger();

  public static void put(String uri, Entry entry) {
    ENTRIES.put(uri, entry);
  }

  /** Number of channels returned by {@code open} that have not been closed yet. */
  public static int openChannels() {
    return OPEN_CHANNELS.get();
  }

  @Override
  public Iterable<FileSystem<?>> fromOptions(PipelineOptions options) {
    return Collections.singletonList(new FakeRemoteFileSystem());
  }

  static final class FakeResourceId implements ResourceId {
    private final String uri;

    FakeResourceId(String uri) {
      this.uri = uri;
    }

    @Override
    public ResourceId resolve(String other, ResolveOptions resolveOptions) {
      return new FakeResourceId(uri.endsWith("/") ? uri + other : uri + "/" + other);
    }

    @Override
    public ResourceId getCurrentDirectory() {
      return new FakeResourceId(uri.substring(0, uri.lastIndexOf('/') + 1));
    }

    @Override
    public String getScheme() {
      return SCHEME;
    }

    @Override
    public String getFilename() {
      return uri.substring(uri.lastIndexOf('/') + 1);
    }

    @Override
    public boolean isDirectory() {
      return uri.endsWith("/");
    }

    @Override
    public String toString() {
      return uri;
    }
  }

  static final class FakeRemoteFileSystem extends FileSystem<FakeResourceId> {
    @Override
    protected List<MatchResult> match(List<String> specs) {
      return specs.stream()
          .map(
              spec -> {
                Entry e = ENTRIES.get(spec);
                if (e == null) {
                  return MatchResult.create(
                      MatchResult.Status.NOT_FOUND, new FileNotFoundException(spec));
                }
                MatchResult.Metadata m =
                    MatchResult.Metadata.builder()
                        .setResourceId(new FakeResourceId(spec))
                        .setSizeBytes(e.declaredSize)
                        .setIsReadSeekEfficient(false)
                        .setLastModifiedMillis(0L)
                        .build();
                return MatchResult.create(MatchResult.Status.OK, Collections.singletonList(m));
              })
          .collect(Collectors.toList());
    }

    @Override
    protected ReadableByteChannel open(FakeResourceId resourceId) throws IOException {
      Entry e = ENTRIES.get(resourceId.toString());
      if (e == null) {
        throw new FileNotFoundException(resourceId.toString());
      }
      OPEN_CHANNELS.incrementAndGet();
      return new ReadableByteChannel() {
        private int pos = 0;
        private boolean open = true;

        @Override
        public int read(ByteBuffer dst) throws IOException {
          if (!open) {
            throw new ClosedChannelException();
          }
          if (e.failAfter >= 0 && pos >= e.failAfter) {
            throw new IOException("Simulated read failure at byte " + pos);
          }
          if (pos >= e.content.length) {
            return -1;
          }
          long limit =
              e.failAfter >= 0 ? Math.min(e.failAfter, e.content.length) : e.content.length;
          int n = (int) Math.min(dst.remaining(), limit - pos);
          dst.put(e.content, pos, n);
          pos += n;
          return n;
        }

        @Override
        public boolean isOpen() {
          return open;
        }

        @Override
        public void close() {
          if (open) {
            open = false;
            OPEN_CHANNELS.decrementAndGet();
          }
        }
      };
    }

    @Override
    protected WritableByteChannel create(FakeResourceId resourceId, CreateOptions createOptions) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void copy(List<FakeResourceId> src, List<FakeResourceId> dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void rename(
        List<FakeResourceId> src, List<FakeResourceId> dst, MoveOptions... moveOptions) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void delete(Collection<FakeResourceId> resourceIds) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected FakeResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
      return new FakeResourceId(singleResourceSpec);
    }

    @Override
    protected String getScheme() {
      return SCHEME;
    }
  }
}
