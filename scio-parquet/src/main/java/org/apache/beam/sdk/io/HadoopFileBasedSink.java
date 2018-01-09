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
import com.google.common.collect.*;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
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
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
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
public abstract class HadoopFileBasedSink<UserT, DestinationT, OutputT>
    implements Serializable, HasDisplayData {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileBasedSink.class);
  static final String TEMP_DIRECTORY_PREFIX = ".temp-beam";

  @Experimental(Kind.FILESYSTEM)
  public static ResourceId convertToFileResourceIfPossible(String outputPrefix) {
    try {
      return FileSystems.matchNewResource(outputPrefix, false /* isDirectory */);
    } catch (Exception e) {
      return FileSystems.matchNewResource(outputPrefix, true /* isDirectory */);
    }
  }

  private final DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations;

  /** The directory to which files will be written. */
  private final ValueProvider<ResourceId> tempDirectoryProvider;

  private static class ExtractDirectory implements SerializableFunction<ResourceId, ResourceId> {
    @Override
    public ResourceId apply(ResourceId input) {
      return input.getCurrentDirectory();
    }
  }

  @Experimental(Kind.FILESYSTEM)
  public HadoopFileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      DynamicDestinations<?, DestinationT, OutputT> dynamicDestinations) {
    this.tempDirectoryProvider =
        NestedValueProvider.of(tempDirectoryProvider, new ExtractDirectory());
    this.dynamicDestinations = checkNotNull(dynamicDestinations);
  }

  /** Return the {@link DynamicDestinations} used. */
  @SuppressWarnings("unchecked")
  public DynamicDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicDestinations<UserT, DestinationT, OutputT>) dynamicDestinations;
  }

  @Experimental(Kind.FILESYSTEM)
  public ValueProvider<ResourceId> getTempDirectoryProvider() {
    return tempDirectoryProvider;
  }

  public void validate(PipelineOptions options) {}

  public abstract WriteOperation<DestinationT, OutputT> createWriteOperation();

  public abstract OutputFileHints getOutputFileHints();

  public void populateDisplayData(DisplayData.Builder builder) {
    getDynamicDestinations().populateDisplayData(builder);
  }

  public abstract static class WriteOperation<DestinationT, OutputT> implements Serializable {
    protected final HadoopFileBasedSink<?, DestinationT, OutputT> sink;

    protected final ValueProvider<ResourceId> tempDirectory;

    @Experimental(Kind.FILESYSTEM)
    protected boolean windowedWrites;

    @Experimental(Kind.FILESYSTEM)
    protected static ResourceId buildTemporaryFilename(ResourceId tempDirectory, String filename)
        throws IOException {
      return tempDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    public WriteOperation(HadoopFileBasedSink<?, DestinationT, OutputT> sink) {
      this(
          sink,
          NestedValueProvider.of(sink.getTempDirectoryProvider(), new TemporaryDirectoryBuilder()));
    }

    private static class TemporaryDirectoryBuilder
        implements SerializableFunction<ResourceId, ResourceId> {
      private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
      private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
          DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");
      // The intent of the code is to have a consistent value of tempDirectory across
      // all workers, which wouldn't happen if now() was called inline.
      private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
      // Multiple different sinks may be used in the same output directory; use tempId to create a
      // separate temp directory for each.
      private final Long tempId = TEMP_COUNT.getAndIncrement();

      @Override
      public ResourceId apply(ResourceId tempDirectory) {
        // Temp directory has a timestamp and a unique ID
        String tempDirName = String.format(TEMP_DIRECTORY_PREFIX + "-%s-%s", timestamp, tempId);
        return tempDirectory
            .getCurrentDirectory()
            .resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    }

    @Experimental(Kind.FILESYSTEM)
    public WriteOperation(HadoopFileBasedSink<?, DestinationT, OutputT> sink, ResourceId tempDirectory) {
      this(sink, StaticValueProvider.of(tempDirectory));
    }

    private WriteOperation(
            HadoopFileBasedSink<?, DestinationT, OutputT> sink, ValueProvider<ResourceId> tempDirectory) {
      this.sink = sink;
      this.tempDirectory = tempDirectory;
      this.windowedWrites = false;
    }

    public abstract Writer<DestinationT, OutputT> createWriter() throws Exception;

    /** Indicates that the operation will be performing windowed writes. */
    public void setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
    }

    public void removeTemporaryFiles(Collection<ResourceId> filenames) throws IOException {
      removeTemporaryFiles(filenames, !windowedWrites);
    }

    @Experimental(Kind.FILESYSTEM)
    protected final List<KV<FileResult<DestinationT>, ResourceId>> finalizeDestination(
        @Nullable DestinationT dest,
        @Nullable BoundedWindow window,
        @Nullable Integer numShards,
        Collection<FileResult<DestinationT>> existingResults) throws Exception {
      Collection<FileResult<DestinationT>> completeResults =
          windowedWrites
              ? existingResults
              : createMissingEmptyShards(dest, numShards, existingResults);

      for (FileResult<DestinationT> res : completeResults) {
        checkArgument(
            Objects.equals(dest, res.getDestination()),
            "File result has wrong destination: expected %s, got %s",
            dest, res.getDestination());
        checkArgument(
            Objects.equals(window, res.getWindow()),
            "File result has wrong window: expected %s, got %s",
            window, res.getWindow());
      }
      List<KV<FileResult<DestinationT>, ResourceId>> outputFilenames = Lists.newArrayList();

      final int effectiveNumShards;
      if (numShards != null) {
        effectiveNumShards = numShards;
        for (FileResult<DestinationT> res : completeResults) {
          checkArgument(
              res.getShard() != UNKNOWN_SHARDNUM,
              "Fixed sharding into %s shards was specified, "
                  + "but file result %s does not specify a shard",
              numShards,
              res);
        }
      } else {
        effectiveNumShards = Iterables.size(completeResults);
        for (FileResult<DestinationT> res : completeResults) {
          checkArgument(
              res.getShard() == UNKNOWN_SHARDNUM,
              "Runner-chosen sharding was specified, "
                  + "but file result %s explicitly specifies a shard",
              res);
        }
      }

      List<FileResult<DestinationT>> resultsWithShardNumbers = Lists.newArrayList();
      if (numShards != null) {
        resultsWithShardNumbers = Lists.newArrayList(completeResults);
      } else {
        int i = 0;
        for (FileResult<DestinationT> res : completeResults) {
          resultsWithShardNumbers.add(res.withShard(i++));
        }
      }

      Map<ResourceId, FileResult<DestinationT>> distinctFilenames = Maps.newHashMap();
      for (FileResult<DestinationT> result : resultsWithShardNumbers) {
        checkArgument(
            result.getShard() != UNKNOWN_SHARDNUM, "Should have set shard number on %s", result);
        ResourceId finalFilename = result.getDestinationFile(
            windowedWrites,
            getSink().getDynamicDestinations(),
            effectiveNumShards,
            getSink().getOutputFileHints());
        checkArgument(
            !distinctFilenames.containsKey(finalFilename),
            "Filename policy must generate unique filenames, but generated the same name %s "
                + "for file results %s and %s",
            finalFilename,
            result,
            distinctFilenames.get(finalFilename));
        distinctFilenames.put(finalFilename, result);
        outputFilenames.add(KV.of(result, finalFilename));
      }
      return outputFilenames;
    }

    private Collection<FileResult<DestinationT>> createMissingEmptyShards(
        @Nullable DestinationT dest,
        @Nullable Integer numShards,
        Collection<FileResult<DestinationT>> existingResults)
        throws Exception {
      Collection<FileResult<DestinationT>> completeResults;
      LOG.info("Finalizing for destination {} num shards {}.", dest, existingResults.size());
      if (numShards != null) {
        checkArgument(
            existingResults.size() <= numShards,
            "Fixed sharding into %s shards was specified, but got %s file results",
            numShards,
            existingResults.size());
      }
      // We must always output at least 1 shard, and honor user-specified numShards
      // if set.
      Set<Integer> missingShardNums;
      if (numShards == null) {
        missingShardNums =
            existingResults.isEmpty()
                ? ImmutableSet.of(UNKNOWN_SHARDNUM)
                : ImmutableSet.<Integer>of();
      } else {
        missingShardNums = Sets.newHashSet();
        for (int i = 0; i < numShards; ++i) {
          missingShardNums.add(i);
        }
        for (FileResult<DestinationT> res : existingResults) {
          checkArgument(
              res.getShard() != UNKNOWN_SHARDNUM,
              "Fixed sharding into %s shards was specified, "
                  + "but file result %s does not specify a shard",
              numShards,
              res);
          missingShardNums.remove(res.getShard());
        }
      }
      completeResults = Lists.newArrayList(existingResults);
      if (!missingShardNums.isEmpty()) {
        LOG.info(
            "Creating {} empty output shards in addition to {} written for destination {}.",
            missingShardNums.size(),
            existingResults.size(),
            dest);
        for (int shard : missingShardNums) {
          String uuid = UUID.randomUUID().toString();
          LOG.info("Opening empty writer {} for destination {}", uuid, dest);
          Writer<DestinationT, ?> writer = createWriter();
          writer.setDestination(dest);
          // Currently this code path is only called in the unwindowed case.
          writer.open(uuid);
          writer.close();
          completeResults.add(
              new FileResult<>(
                  writer.getOutputFile(),
                  shard,
                  GlobalWindow.INSTANCE,
                  PaneInfo.ON_TIME_AND_ONLY_FIRING,
                  dest));
        }
        LOG.debug("Done creating extra shards for {}.", dest);
      }
      return completeResults;
    }

    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
    final void moveToOutputFiles(
        List<KV<FileResult<DestinationT>, ResourceId>> resultsToFinalFilenames) throws IOException {
      int numFiles = resultsToFinalFilenames.size();

        LOG.debug("Copying {} files.", numFiles);
        List<ResourceId> srcFiles = new ArrayList<>();
        List<ResourceId> dstFiles = new ArrayList<>();
        for (KV<FileResult<DestinationT>, ResourceId> entry : resultsToFinalFilenames) {
          srcFiles.add(entry.getKey().getTempFilename());
          dstFiles.add(entry.getValue());
          LOG.info(
              "Will copy temporary file {} to final location {}",
              entry.getKey(),
              entry.getValue());
        }
        // During a failure case, files may have been deleted in an earlier step. Thus
        // we ignore missing files here.
        FileSystems.copy(srcFiles, dstFiles, StandardMoveOptions.IGNORE_MISSING_FILES);
      removeTemporaryFiles(srcFiles);
    }

    @VisibleForTesting
    @Experimental(Kind.FILESYSTEM)
    final void removeTemporaryFiles(
        Collection<ResourceId> knownFiles, boolean shouldRemoveTemporaryDirectory)
        throws IOException {
      ResourceId tempDir = tempDirectory.get();
      LOG.debug("Removing temporary bundle output files in {}.", tempDir);

      // To partially mitigate the effects of filesystems with eventually-consistent
      // directory matching APIs, we remove not only files that the filesystem says exist
      // in the directory (which may be incomplete), but also files that are known to exist
      // (produced by successfully completed bundles).

      // This may still fail to remove temporary outputs of some failed bundles, but at least
      // the common case (where all bundles succeed) is guaranteed to be fully addressed.
      Set<ResourceId> allMatches = new HashSet<>(knownFiles);
      for (ResourceId match : allMatches) {
        LOG.info("Will remove known temporary file {}", match);
      }
      // TODO: Windows OS cannot resolves and matches '*' in the path,
      // ignore the exception for now to avoid failing the pipeline.
      if (shouldRemoveTemporaryDirectory) {
        try {
          MatchResult singleMatch =
              Iterables.getOnlyElement(
                  FileSystems.match(Collections.singletonList(tempDir.toString() + "*")));
          for (Metadata matchResult : singleMatch.metadata()) {
            if (allMatches.add(matchResult.resourceId())) {
              LOG.info("Will also remove unknown temporary file {}", matchResult.resourceId());
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to match temporary files under: [{}].", tempDir);
        }
      }
      FileSystems.delete(allMatches, StandardMoveOptions.IGNORE_MISSING_FILES);

      if (shouldRemoveTemporaryDirectory) {
        // Deletion of the temporary directory might fail, if not all temporary files are removed.
        try {
          FileSystems.delete(
              Collections.singletonList(tempDir), StandardMoveOptions.IGNORE_MISSING_FILES);
        } catch (Exception e) {
          LOG.warn("Failed to remove temporary directory: [{}].", tempDir);
        }
      }
    }

    public HadoopFileBasedSink<?, DestinationT, OutputT> getSink() {
      return sink;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + "{"
          + "tempDirectory="
          + tempDirectory
          + ", windowedWrites="
          + windowedWrites
          + '}';
    }
  }

  public abstract static class Writer<DestinationT, OutputT> {
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final WriteOperation<DestinationT, OutputT> writeOperation;

    private @Nullable String id;
    private @Nullable DestinationT destination;
    private @Nullable ResourceId outputFile;
    private @Nullable Path path;

    @Nullable private final String mimeType;

    public Writer(WriteOperation<DestinationT, OutputT> writeOperation) {
      checkNotNull(writeOperation);
      this.writeOperation = writeOperation;
      this.mimeType = writeOperation.getSink().getOutputFileHints().getMimeType();
    }

    protected abstract void prepareWrite(Path path) throws Exception;

    protected void finishWrite() throws Exception {}

    public final void open(String uId) throws Exception {
      this.id = uId;
      ResourceId tempDirectory = getWriteOperation().tempDirectory.get();
      outputFile = tempDirectory.resolve(id, StandardResolveOptions.RESOLVE_FILE);
      verifyNotNull(
          outputFile, "FileSystems are not allowed to return null from resolve: %s", tempDirectory);

      path = new Path(outputFile.toString());

      LOG.debug("Preparing write to {}.", outputFile);
      prepareWrite(path);

      LOG.debug("Starting write of bundle {} to {}.", this.id, outputFile);
    }

    public abstract void write(OutputT value) throws Exception;

    public ResourceId getOutputFile() {
      return outputFile;
    }

    public final void cleanup() throws Exception {
      if (outputFile != null) {
        LOG.info("Deleting temporary file {}", outputFile);
        // outputFile may be null if open() was not called or failed.
        FileSystems.delete(
                Collections.singletonList(outputFile), StandardMoveOptions.IGNORE_MISSING_FILES);
      }
    }

    public final void close() throws Exception {
      checkState(outputFile != null, "FileResult.close cannot be called with a null outputFile");
      LOG.debug("Closing {}", outputFile);

      finishWrite();

      LOG.info("Successfully wrote temporary file {}", outputFile);
    }

    public WriteOperation<DestinationT, OutputT> getWriteOperation() {
      return writeOperation;
    }

    void setDestination(DestinationT destination) {
      this.destination = destination;
    }

    public DestinationT getDestination() {
      return destination;
    }
  }
}
