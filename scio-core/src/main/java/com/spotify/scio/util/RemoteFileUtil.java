/*
 * Copyright 2017 Spotify AB.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.MapMaker;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * A utility class for handling remote file systems designed to be used in a
 * {@link org.apache.beam.sdk.transforms.DoFn}.
 */
public abstract class RemoteFileUtil implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteFileUtil.class);

  private static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors() * 4;
  private static final int HASH_LENGTH = 8;

  // Mapping of remote sources to local destinations
  private static final ConcurrentMap<URI, Path> paths =
      new MapMaker()
          .concurrencyLevel(CONCURRENCY_LEVEL)
          .initialCapacity(CONCURRENCY_LEVEL * 8)
          .makeMap();

  // Globally shared thread pool
  private static final ExecutorService executorService = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(CONCURRENCY_LEVEL));

  /**
   * Create a new {@link RemoteFileUtil} instance.
   */
  public static RemoteFileUtil create(PipelineOptions options) {
    // FIXME: how to handle other file systems?
    try {
      return new GcsRemoteFileUtil(options.as(GcsOptions.class));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Check if a remote {@link URI} exists.
   */
  public abstract boolean remoteExists(URI uri) throws IOException;

  /**
   * Download a single remote {@link URI}.
   * @return {@link Path} to the downloaded local file.
   */
  public Path download(URI src) {
    // Blocks on globally shared ConcurrentMap with concurrency determined by CONCURRENCY_LEVEL
    return paths.computeIfAbsent(src, this::downloadImpl);
  }

  /**
   * Download a batch of remote {@link URI}s in parallel.
   * @return {@link Path}s to the downloaded local files.
   */
  public List<Path> download(List<URI> srcs) {
    // Blocks on globally shared ConcurrentMap with concurrency determined by CONCURRENCY_LEVEL
    synchronized (paths) {
      List<URI> missing = srcs.stream()
          .filter(src -> !paths.containsKey(src))
          .collect(Collectors.toList());

      List<CompletableFuture<Path>> futures = missing.stream()
          .map(uri -> CompletableFuture.supplyAsync(() -> downloadImpl(uri), executorService))
          .collect(Collectors.toList());

      try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        List<Path> result = futures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList());

        Iterator<URI> i = missing.iterator();
        Iterator<Path> j = result.iterator();
        while (i.hasNext() && j.hasNext()) {
          paths.put(i.next(), j.next());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while executing batch download request", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Error executing batch download request", e);
      }
    }

    return srcs.stream().map(paths::get).collect(Collectors.toList());
  }

  /**
   * Delete a single downloaded local file.
   */
  public void delete(URI src) {
    Path dst = paths.remove(src);
    try {
      Files.deleteIfExists(dst);
    } catch (IOException e) {
      String msg = String.format("Failed to delete %s -> %s", src, dst);
      LOG.error(msg, e);
    }
  }

  /**
   * Delete a batch of downloaded local files.
   */
  public void delete(List<URI> srcs) {
    srcs.forEach(this::delete);
  }

  /**
   * Upload a single local {@link Path} to a remote {@link URI}.
   */
  public void upload(Path src, URI dst) throws IOException {
    boolean exists = true;
    try {
      getRemoteSize(dst);
    } catch (IOException e) {
      if (e instanceof FileNotFoundException) {
        exists = false;
      } else {
        throw e;
      }
    }
    if (exists) {
      String msg = String.format("Destination URI %s already exists", dst);
      throw new IllegalArgumentException(msg);
    }
    copyToRemote(src, dst);
  }

  private Path downloadImpl(URI src) {
    try {
      Path dst = getDestination(src);

      if (src.getScheme() == null || src.getScheme().equals("file")) {
        // Local URI
        Path srcPath = src.getScheme() == null ? Paths.get(src.toString()) : Paths.get(src);
        if (Files.isSymbolicLink(dst) && Files.readSymbolicLink(dst).equals(srcPath)) {
          LOG.info("URI {} already symlink-ed", src);
        } else {
          Files.createSymbolicLink(dst, srcPath);
          LOG.info("Symlink-ed {} to {}", src, dst);
        }
      } else {
        // Remote URI
        long srcSize = getRemoteSize(src);
        boolean shouldDownload = true;
        if (Files.exists(dst)) {
          long dstSize = Files.size(dst);
          if (srcSize == dstSize) {
            LOG.info("URI {} already downloaded", src);
            shouldDownload = false;
          } else {
            LOG.warn("Destination exists with wrong size. {} [{}B] -> {} [{}B]",
                src, srcSize, dst, dstSize);
            Files.delete(dst);
          }
        }
        if (shouldDownload) {
          copyToLocal(src, dst);
          LOG.info("Downloaded {} -> {} [{}B]", src, dst, srcSize);
        }
      }

      return dst;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path getDestination(URI src) throws IOException {
    String scheme = src.getScheme();
    if (scheme == null) {
      scheme = "file";
    }

    // Sparkey expects a pair of ".spi" and ".spl" files with the same path. Hash URI prefix
    // before filename so that URIs with the same remote path have the same local one. E.g.
    // gs://bucket/path/data.spi -> /tmp/fd-gs-a1b2c3d4/data.spi
    // gs://bucket/path/data.spl -> /tmp/fd-gs-a1b2c3d4/data.spl
    String path = src.toString();
    int idx = path.lastIndexOf('/');
    String hash = Hashing.sha1()
        .hashString(path.substring(0, idx), Charsets.UTF_8).toString()
        .substring(0, HASH_LENGTH);
    String filename = path.substring(idx + 1);

    String tmpDir = System.getProperties().getProperty("java.io.tmpdir");
    Path parent = Paths.get(tmpDir, String.format("fd-%s-%s", scheme, hash));
    Files.createDirectories(parent);
    return parent.resolve(filename);
  }

  // Copy a single file from remote source to local destination
  protected abstract void copyToLocal(URI src, Path dst) throws IOException;

  // Copy a single file from local source to remote destination
  protected abstract void copyToRemote(Path src, URI dst) throws IOException;

  // Get size of a remote URI
  protected abstract long getRemoteSize(URI src) throws IOException;

  // =======================================================================
  // Implementations
  // =======================================================================

  private static class GcsRemoteFileUtil extends RemoteFileUtil {

    // GcsOptions is not serializable
    private final String jsonGcsOptions;

    // GcsUtil is not serializable
    private final Supplier<GcsUtil> gcsUtil =
        Suppliers.memoize((Supplier<GcsUtil> & Serializable) this::createGcsUtil);

    GcsRemoteFileUtil(GcsOptions gcsOptions) throws JsonProcessingException {
      jsonGcsOptions = new ObjectMapper().writeValueAsString(gcsOptions);
    }

    private GcsUtil createGcsUtil() {
      try {
        return new ObjectMapper()
            .readValue(jsonGcsOptions, PipelineOptions.class)
            .as(GcsOptions.class)
            .getGcsUtil();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean remoteExists(URI uri) throws IOException {
      try {
        getRemoteSize(uri);
        return true;
      } catch (IOException e) {
        if (e instanceof FileNotFoundException) {
          return false;
        }
        throw e;
      }
    }

    @Override
    protected void copyToLocal(URI src, Path dst) throws IOException {
      FileChannel dstCh = FileChannel.open(
          dst, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
      SeekableByteChannel srcCh = gcsUtil.get().open(GcsPath.fromUri(src));
      long srcSize = srcCh.size();
      long copied = 0;
      do {
        copied += dstCh.transferFrom(srcCh, copied, srcSize - copied);
      } while (copied < srcSize);
      dstCh.close();
      srcCh.close();
      Preconditions.checkState(copied == srcSize);
    }

    @Override
    protected void copyToRemote(Path src, URI dst) throws IOException {
      WritableByteChannel dstCh = gcsUtil.get().create(GcsPath.fromUri(dst), MimeTypes.BINARY);
      FileChannel srcCh = FileChannel.open(src, StandardOpenOption.READ);
      long srcSize = srcCh.size();
      long copied = 0;
      do {
        copied += srcCh.transferTo(copied, srcSize - copied, dstCh);
      } while (copied < srcSize);
      dstCh.close();
      srcCh.close();
      Preconditions.checkState(copied == srcSize);
    }

    @Override
    protected long getRemoteSize(URI src) throws IOException {
      return gcsUtil.get().fileSize(GcsPath.fromUri(src));
    }

  }

}
