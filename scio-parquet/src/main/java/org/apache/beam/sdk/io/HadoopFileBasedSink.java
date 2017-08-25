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
package org.apache.beam.sdk.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.hadoop.fs.Path;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Verify.verifyNotNull;
import static org.apache.beam.sdk.io.WriteFiles.UNKNOWN_SHARDNUM;

@Experimental(Kind.FILESYSTEM)
public abstract class HadoopFileBasedSink<T> implements Serializable, HasDisplayData {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileBasedSink.class);

  private final FilenamePolicy filenamePolicy;
  private final ValueProvider<ResourceId> baseOutputDirectoryProvider;

  @Experimental(Kind.FILESYSTEM)
  public HadoopFileBasedSink(
      ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy) {
    this.baseOutputDirectoryProvider =
        NestedValueProvider.of(baseOutputDirectoryProvider, new ExtractDirectory());
    this.filenamePolicy = filenamePolicy;
  }

  private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
    @Override
    public ResourceId apply(ResourceId input) {
      return input.getCurrentDirectory();
    }
  }

  @Experimental(Kind.FILESYSTEM)
  public ValueProvider<ResourceId> getBaseOutputDirectoryProvider() {
    return baseOutputDirectoryProvider;
  }

  @Experimental(Kind.FILESYSTEM)
  public final FilenamePolicy getFilenamePolicy() {
    return filenamePolicy;
  }

  public void validate(PipelineOptions options) {}

  public abstract WriteOperation<T> createWriteOperation();

  public void populateDisplayData(DisplayData.Builder builder) {
    getFilenamePolicy().populateDisplayData(builder);
  }

  public abstract static class WriteOperation<T> implements Serializable {
    protected final HadoopFileBasedSink<T> sink;

    protected final ValueProvider<ResourceId> tempDirectory;

    @Experimental(Kind.FILESYSTEM)
    protected boolean windowedWrites;

    @Experimental(Kind.FILESYSTEM)
    protected static ResourceId buildTemporaryFilename(ResourceId tempDirectory, String filename)
        throws IOException {
      return tempDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    public WriteOperation(HadoopFileBasedSink<T> sink) {
      this(sink, NestedValueProvider.of(
          sink.getBaseOutputDirectoryProvider(), new TemporaryDirectoryBuilder()));
    }

    private static class TemporaryDirectoryBuilder
        implements SerializableFunction<ResourceId, ResourceId> {
      private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
      private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
          DateTimeFormat.forPattern("yyyy-MM-DD_HH-mm-ss");
      // The intent of the code is to have a consistent value of tempDirectory across
      // all workers, which wouldn't happen if now() was called inline.
      private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
      // Multiple different sinks may be used in the same output directory; use tempId to create a
      // separate temp directory for each.
      private final Long tempId = TEMP_COUNT.getAndIncrement();

      @Override
      public ResourceId apply(ResourceId baseOutputDirectory) {
        // Temp directory has a timestamp and a unique ID
        String tempDirName = String.format(".temp-beam-%s-%s", timestamp, tempId);
        return baseOutputDirectory.resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    }

    @Experimental(Kind.FILESYSTEM)
    public WriteOperation(HadoopFileBasedSink<T> sink, ResourceId tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory));
    }

    private WriteOperation(
        HadoopFileBasedSink<T> sink, ValueProvider<ResourceId> tempDirectory) {
      this.sink = sink;
      this.tempDirectory = tempDirectory;
      this.windowedWrites = false;
    }

    public abstract Writer<T> createWriter() throws Exception;

    public void setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
    }

    public void finalize(Iterable<FileResult> writerResults) throws Exception {
      // Collect names of temporary files and rename them.
      Map<ResourceId, ResourceId> outputFilenames = buildOutputFilenames(writerResults);
      copyToOutputFiles(outputFilenames);

      // Optionally remove temporary files.
      // We remove the entire temporary directory, rather than specifically removing the files
      // from writerResults, because writerResults includes only successfully completed bundles,
      // and we'd like to clean up the failed ones too.
      // Note that due to GCS eventual consistency, matching files in the temp directory is also
      // currently non-perfect and may fail to delete some files.
      //
      // When windows or triggers are specified, files are generated incrementally so deleting
      // the entire directory in finalize is incorrect.
      removeTemporaryFiles(outputFilenames.keySet(), !windowedWrites);
    }

    @Experimental(Kind.FILESYSTEM)
    protected final Map<ResourceId, ResourceId> buildOutputFilenames(
        Iterable<FileResult> writerResults) {
      int numShards = Iterables.size(writerResults);
      Map<ResourceId, ResourceId> outputFilenames = new HashMap<>();

      FilenamePolicy policy = getSink().getFilenamePolicy();
      ResourceId baseOutputDir = getSink().getBaseOutputDirectoryProvider().get();

      // Either all results have a shard number set (if the sink is configured with a fixed
      // number of shards), or they all don't (otherwise).
      Boolean isShardNumberSetEverywhere = null;
      for (FileResult result : writerResults) {
        boolean isShardNumberSetHere = (result.getShard() != UNKNOWN_SHARDNUM);
        if (isShardNumberSetEverywhere == null) {
          isShardNumberSetEverywhere = isShardNumberSetHere;
        } else {
          checkArgument(
              isShardNumberSetEverywhere == isShardNumberSetHere,
              "Found a mix of files with and without shard number set: %s",
              result);
        }
      }

      if (isShardNumberSetEverywhere == null) {
        isShardNumberSetEverywhere = true;
      }

      List<FileResult> resultsWithShardNumbers = Lists.newArrayList();
      if (isShardNumberSetEverywhere) {
        resultsWithShardNumbers = Lists.newArrayList(writerResults);
      } else {
        // Sort files for idempotence. Sort by temporary filename.
        // Note that this codepath should not be used when processing triggered windows. In the
        // case of triggers, the list of FileResult objects in the Finalize iterable is not
        // deterministic, and might change over retries. This breaks the assumption below that
        // sorting the FileResult objects provides idempotency.
        List<FileResult> sortedByTempFilename =
            Ordering.from(
                new Comparator<FileResult>() {
                  @Override
                  public int compare(FileResult first, FileResult second) {
                    String firstFilename = first.getTempFilename().toString();
                    String secondFilename = second.getTempFilename().toString();
                    return firstFilename.compareTo(secondFilename);
                  }
                })
                .sortedCopy(writerResults);
        for (int i = 0; i < sortedByTempFilename.size(); i++) {
          resultsWithShardNumbers.add(sortedByTempFilename.get(i).withShard(i));
        }
      }

      for (FileResult result : resultsWithShardNumbers) {
        checkArgument(
            result.getShard() != UNKNOWN_SHARDNUM, "Should have set shard number on %s", result);
        outputFilenames.put(
            result.getTempFilename(),
            result.getDestinationFile(
                policy, baseOutputDir, numShards, ""));
      }

      int numDistinctShards = new HashSet<>(outputFilenames.values()).size();
      checkState(numDistinctShards == outputFilenames.size(),
          "Only generated %s distinct file names for %s files.",
          numDistinctShards, outputFilenames.size());

      return outputFilenames;
    }

    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
    final void copyToOutputFiles(Map<ResourceId, ResourceId> filenames)
        throws IOException {
      int numFiles = filenames.size();
      if (numFiles > 0) {
        LOG.debug("Copying {} files.", numFiles);
        List<ResourceId> srcFiles = new ArrayList<>(filenames.size());
        List<ResourceId> dstFiles = new ArrayList<>(filenames.size());
        for (Map.Entry<ResourceId, ResourceId> srcDestPair : filenames.entrySet()) {
          srcFiles.add(srcDestPair.getKey());
          dstFiles.add(srcDestPair.getValue());
        }
        // During a failure case, files may have been deleted in an earlier step. Thus
        // we ignore missing files here.
        FileSystems.copy(srcFiles, dstFiles, StandardMoveOptions.IGNORE_MISSING_FILES);
      } else {
        LOG.info("No output files to write.");
      }
    }

    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
    final void removeTemporaryFiles(
        Set<ResourceId> knownFiles, boolean shouldRemoveTemporaryDirectory) throws IOException {
      ResourceId tempDir = tempDirectory.get();
      LOG.debug("Removing temporary bundle output files in {}.", tempDir);

      // To partially mitigate the effects of filesystems with eventually-consistent
      // directory matching APIs, we remove not only files that the filesystem says exist
      // in the directory (which may be incomplete), but also files that are known to exist
      // (produced by successfully completed bundles).

      // This may still fail to remove temporary outputs of some failed bundles, but at least
      // the common case (where all bundles succeed) is guaranteed to be fully addressed.
      Set<ResourceId> matches = new HashSet<>();
      // TODO: Windows OS cannot resolves and matches '*' in the path,
      // ignore the exception for now to avoid failing the pipeline.
      if (shouldRemoveTemporaryDirectory) {
        try {
          MatchResult singleMatch = Iterables.getOnlyElement(
              FileSystems.match(Collections.singletonList(tempDir.toString() + "*")));
          for (Metadata matchResult : singleMatch.metadata()) {
            matches.add(matchResult.resourceId());
          }
        } catch (Exception e) {
          LOG.warn("Failed to match temporary files under: [{}].", tempDir);
        }
      }
      Set<ResourceId> allMatches = new HashSet<>(matches);
      allMatches.addAll(knownFiles);
      LOG.debug(
          "Removing {} temporary files found under {} ({} matched glob, {} known files)",
          allMatches.size(),
          tempDir,
          matches.size(),
          allMatches.size() - matches.size());
      FileSystems.delete(allMatches, StandardMoveOptions.IGNORE_MISSING_FILES);

      // Deletion of the temporary directory might fail, if not all temporary files are removed.
      try {
        FileSystems.delete(
            Collections.singletonList(tempDir), StandardMoveOptions.IGNORE_MISSING_FILES);
      } catch (Exception e) {
        LOG.warn("Failed to remove temporary directory: [{}].", tempDir);
      }
    }

    public HadoopFileBasedSink<T> getSink() {
      return sink;
    }

    @Override
    public String toString() {
      String tempDirectoryStr =
          tempDirectory.isAccessible() ? tempDirectory.get().toString() : tempDirectory.toString();
      return getClass().getSimpleName()
          + "{"
          + "tempDirectory="
          + tempDirectoryStr
          + ", windowedWrites="
          + windowedWrites
          + '}';
    }
  }

  public abstract static class Writer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final WriteOperation<T> writeOperation;

    private String id;

    private BoundedWindow window;
    private PaneInfo paneInfo;
    private int shard = -1;

    private @Nullable ResourceId outputFile;

    private Path path;

    public Writer(WriteOperation<T> writeOperation) {
      checkNotNull(writeOperation);
      this.writeOperation = writeOperation;
    }

    protected abstract void prepareWrite(Path path) throws Exception;

    protected void finishWrite() throws Exception {}

    public final void openWindowed(String uId, BoundedWindow window, PaneInfo paneInfo, int shard)
        throws Exception {
      if (!getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openWindowed called a non-windowed sink.");
      }
      open(uId, window, paneInfo, shard);
    }

    public abstract void write(T value) throws Exception;

    public final void openUnwindowed(String uId, int shard) throws Exception {
      if (getWriteOperation().windowedWrites) {
        throw new IllegalStateException("openUnwindowed called a windowed sink.");
      }
      open(uId, null, null, shard);
    }

    private void open(String uId,
                      @Nullable BoundedWindow window,
                      @Nullable PaneInfo paneInfo,
                      int shard) throws Exception {
      this.id = uId;
      this.window = window;
      this.paneInfo = paneInfo;
      this.shard = shard;
      ResourceId tempDirectory = getWriteOperation().tempDirectory.get();
      outputFile = tempDirectory.resolve(id, StandardResolveOptions.RESOLVE_FILE);
      verifyNotNull(
          outputFile, "FileSystems are not allowed to return null from resolve: %s", tempDirectory);

      LOG.debug("Opening {} for write", outputFile);
      path = new Path(outputFile.toString());


      // The caller shouldn't have to close() this Writer if it fails to open(), so close
      // the channel if prepareWrite() or writeHeader() fails.
      LOG.debug("Preparing write to {}.", outputFile);
      prepareWrite(path);

      LOG.debug("Starting write of bundle {} to {}.", this.id, outputFile);
    }

    public final void cleanup() throws Exception {
      if (outputFile != null) {
        // outputFile may be null if open() was not called or failed.
        FileSystems.delete(
            Collections.singletonList(outputFile), StandardMoveOptions.IGNORE_MISSING_FILES);
      }
    }

    public final FileResult close() throws Exception {
      checkState(outputFile != null, "FileResult.close cannot be called with a null outputFile");

      LOG.debug("Finishing write to {}.", outputFile);
      finishWrite();

      FileResult result = new FileResult(outputFile, shard, window, paneInfo);
      LOG.debug("Result for bundle {}: {}", this.id, outputFile);
      return result;
    }

    public WriteOperation<T> getWriteOperation() {
      return writeOperation;
    }
  }

}
