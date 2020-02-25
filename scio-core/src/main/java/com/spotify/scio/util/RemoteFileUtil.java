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

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A utility class for handling remote file systems designed to be used in a
 * {@link org.apache.beam.sdk.transforms.DoFn}.
 */
public class RemoteFileUtil implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteFileUtil.class);

  private static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors() * 4;
  private static final int HASH_LENGTH = 8;

  // Mapping of remote sources to local destinations
  private static final LoadingCache<URI, Path> paths = CacheBuilder.newBuilder()
      .concurrencyLevel(CONCURRENCY_LEVEL)
      .initialCapacity(CONCURRENCY_LEVEL * 8)
      .build(new CacheLoader<URI, Path>() {
        @Override
        public Path load(URI key) throws Exception {
          return downloadImpl(key);
        }
      });

  /**
   * Create a new {@link RemoteFileUtil} instance.
   */
  public static RemoteFileUtil create(PipelineOptions options) {
    FileSystems.setDefaultPipelineOptions(options);
    return new RemoteFileUtil();
  }

  /**
   * Check if a remote {@link URI} exists.
   */
  public boolean remoteExists(URI uri) throws IOException {
    try {
      FileSystems.matchSingleFileSpec(uri.toString());
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Download a single remote {@link URI}.
   * @return {@link Path} to the downloaded local file.
   */
  public Path download(URI src) {
    try {
      return paths.get(src);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Download a batch of remote {@link URI}s in parallel.
   * @return {@link Path}s to the downloaded local files.
   */
  public List<Path> download(List<URI> srcs) {
    try {
      return paths.getAll(srcs).values().asList();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Delete a single downloaded local file.
   */
  public void delete(URI src) {
    Path dst = null;
    try {
      dst = paths.get(src);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    try {
      Files.deleteIfExists(dst);
      paths.invalidate(src);
    } catch (IOException e) {
      String msg = String.format("Failed to delete %s -> %s", src, dst);
      LOG.error(msg, e);
    }
  }

  /**
   * Delete a batch of downloaded local files.
   */
  public void delete(List<URI> srcs) {
    for (URI src : srcs) {
      delete(src);
    }
    paths.invalidateAll(srcs);
  }

  /**
   * Upload a single local {@link Path} to a remote {@link URI}.
   */
  public void upload(Path src, URI dst) throws IOException {
    upload(src, dst, MimeTypes.BINARY);
  }

  /**
   * Upload a single local {@link Path} to a remote {@link URI} with mimeType {@link MimeTypes}.
   */
  public void upload(Path src, URI dst, String mimeType) throws IOException {
    if (remoteExists(dst)) {
      String msg = String.format("Destination URI %s already exists", dst);
      throw new IllegalArgumentException(msg);
    }
    copyToRemote(src, dst, mimeType);
  }

  private static Path downloadImpl(URI src) {
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
        Metadata srcMeta = getMetadata(src);
        long srcSize = srcMeta.sizeBytes();
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
          copyToLocal(srcMeta, dst);
          LOG.info("Downloaded {} -> {} [{}B]", src, dst, srcSize);
        }
      }

      return dst;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Path getDestination(URI src) throws IOException {
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
    String hash = Hashing.murmur3_128()
        .hashString(path.substring(0, idx), Charsets.UTF_8).toString()
        .substring(0, HASH_LENGTH);
    String filename = path.substring(idx + 1);

    String tmpDir = System.getProperties().getProperty("java.io.tmpdir");
    Path parent = Paths.get(tmpDir, String.format("fd-%s-%s", scheme, hash));
    Files.createDirectories(parent);
    return parent.resolve(filename);
  }

  // Copy a single file from remote source to local destination
  private static void copyToLocal(Metadata src, Path dst) throws IOException {
    FileChannel dstCh = FileChannel.open(
        dst, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    ReadableByteChannel srcCh = FileSystems.open(src.resourceId());
    long srcSize = src.sizeBytes();
    long copied = 0;
    do {
      copied += dstCh.transferFrom(srcCh, copied, srcSize - copied);
    } while (copied < srcSize);
    dstCh.close();
    srcCh.close();
    Preconditions.checkState(copied == srcSize);
  }

  // Copy a single file from local source to remote destination
  private static void copyToRemote(Path src, URI dst, String mimeType) throws IOException {
    ResourceId dstId = FileSystems.matchNewResource(dst.toString(), false);
    WritableByteChannel dstCh = FileSystems.create(dstId, mimeType);
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

  private static Metadata getMetadata(URI src) throws IOException {
    return FileSystems.matchSingleFileSpec(src.toString());
  }

}
