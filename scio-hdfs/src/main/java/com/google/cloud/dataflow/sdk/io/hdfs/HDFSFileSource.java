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
package com.google.cloud.dataflow.sdk.io.hdfs;

import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class HDFSFileSource<T, K, V> extends BoundedSource<T> {
  private static final long serialVersionUID = 0L;

  private final String filepattern;
  private final Class<? extends FileInputFormat<K, V>> formatClass;
  private final Coder<T> coder;
  private final SerializableFunction<KV<K, V>, T> inputConverter;
  private final SerializableConfiguration serializableConfiguration;
  private final SerializableSplit serializableSplit;
  private final boolean validate;

  private HDFSFileSource(String filepattern,
                         Class<? extends FileInputFormat<?, ?>> formatClass,
                         Coder<T> coder,
                         SerializableFunction<KV<K, V>, T> inputConverter,
                         SerializableConfiguration serializableConfiguration,
                         SerializableSplit serializableSplit,
                         boolean validate) {
    this.filepattern = filepattern;
    this.formatClass = castClass(formatClass);
    this.coder = coder;
    this.inputConverter = inputConverter;
    this.serializableConfiguration = serializableConfiguration;
    this.serializableSplit = serializableSplit;
    this.validate = validate;
  }

  // =======================================================================
  // Factory methods
  // =======================================================================

  public static <T, K, V, F extends FileInputFormat<K, V>> HDFSFileSource<T, K, V>
  from(String filepattern,
       Class<F> formatClass,
       Coder<T> coder,
       SerializableFunction<KV<K, V>, T> inputConverter) {
    return new HDFSFileSource<>(filepattern, formatClass, coder, inputConverter, null, null, true);
  }

  public static <K, V, F extends FileInputFormat<K, V>> HDFSFileSource<KV<K, V>, K, V>
  from(String filepattern,
       Class<F> formatClass,
       Class<K> keyClass,
       Class<V> valueClass) {
    KvCoder<K, V> coder = KvCoder.of(getDefaultCoder(keyClass), getDefaultCoder(valueClass));
    SerializableFunction<KV<K, V>, KV<K, V>> inputConverter =
        new SerializableFunction<KV<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> apply(KV<K, V> input) {
            return input;
          }
        };
    return new HDFSFileSource<>(filepattern, formatClass, coder, inputConverter, null, null, true);
  }

  public static HDFSFileSource<String, LongWritable, Text>
  fromText(String filepattern) {
    SerializableFunction<KV<LongWritable, Text>, String> inputConverter =
        new SerializableFunction<KV<LongWritable, Text>, String>() {
      @Override
      public String apply(KV<LongWritable, Text> input) {
        return input.getValue().toString();
      }
    };
    return from(filepattern, TextInputFormat.class, StringUtf8Coder.of(), inputConverter);
  }

  public static <T> HDFSFileSource<T, AvroKey<T>, NullWritable>
  fromAvro(String filepattern, final AvroCoder<T> coder) {
    Class<AvroKeyInputFormat<T>> formatClass = castClass(AvroKeyInputFormat.class);
    SerializableFunction<KV<AvroKey<T>, NullWritable>, T> inputConverter =
        new SerializableFunction<KV<AvroKey<T>, NullWritable>, T>() {
          @Override
          public T apply(KV<AvroKey<T>, NullWritable> input) {
            try {
              return CoderUtils.clone(coder, input.getKey().datum());
            } catch (CoderException e) {
              throw new RuntimeException(e);
            }
          }
        };
    Configuration conf = new Configuration();
    conf.set("avro.schema.input.key", coder.getSchema().toString());
    return from(filepattern, formatClass, coder, inputConverter).withConfiguration(conf);
  }

  public static HDFSFileSource<GenericRecord, AvroKey<GenericRecord>, NullWritable>
  fromAvro(String filepattern, Schema schema) {
    return fromAvro(filepattern, AvroCoder.of(schema));
  }

  public static <T> HDFSFileSource<T, AvroKey<T>, NullWritable>
  fromAvro(String filepattern, Class<T> cls) {
    return fromAvro(filepattern, AvroCoder.of(cls));
  }

  // =======================================================================
  // Builder methods
  // =======================================================================

  public HDFSFileSource<T, K, V> withCoder(Coder<T> coder) {
    return new HDFSFileSource<>(
        filepattern, formatClass, coder, inputConverter,
        serializableConfiguration, serializableSplit, validate);
  }

  public HDFSFileSource<T, K, V> withInputConverter(
      SerializableFunction<KV<K, V>, T> inputConverter) {
    return new HDFSFileSource<>(
        filepattern, formatClass, coder, inputConverter,
        serializableConfiguration, serializableSplit, validate);
  }

  public HDFSFileSource<T, K, V> withConfiguration(Configuration conf) {
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(conf);
    return new HDFSFileSource<>(
        filepattern, formatClass, coder, inputConverter,
        serializableConfiguration, serializableSplit, validate);
  }

  public HDFSFileSource<T, K, V> withInputSplit(InputSplit inputSplit) {
    SerializableSplit serializableSplit = new SerializableSplit(inputSplit);
    return new HDFSFileSource<>(
        filepattern, formatClass, coder, inputConverter,
        serializableConfiguration, serializableSplit, validate);
  }

  public HDFSFileSource<T, K, V> withoutValidation() {
    return new HDFSFileSource<>(
        filepattern, formatClass, coder, inputConverter,
        serializableConfiguration, serializableSplit, false);
  }

  // =======================================================================
  // BoundedSource
  // =======================================================================

  @Override
  public List<? extends BoundedSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<T>>() {
            @Override
            public BoundedSource<T> apply(@Nullable InputSplit inputSplit) {
              SerializableSplit serializableSplit = new SerializableSplit(inputSplit);
              return new HDFSFileSource<>(
                  filepattern, formatClass, coder, inputConverter,
                  serializableConfiguration, serializableSplit, validate);
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    long size = 0;
    Job job = Job.getInstance(); // new instance
    for (FileStatus st : listStatus(createFormat(job), job)) {
      size += st.getLen();
    }
    return size;
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    this.validate();
    return new HDFSFileReader<>(this, filepattern, formatClass, serializableSplit);
  }

  @Override
  public void validate() {
    if (validate) {
      try {
        FileSystem fs = FileSystem.get(new URI(filepattern), Job.getInstance().getConfiguration());
        FileStatus[] fileStatuses = fs.globStatus(new Path(filepattern));
        checkState(
            fileStatuses != null && fileStatuses.length > 0,
            "Unable to find any files matching %s", filepattern);
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return coder;
  }

  // =======================================================================
  // Helpers
  // =======================================================================

  private List<InputSplit> computeSplits(long desiredBundleSizeBytes)
      throws IOException, IllegalAccessException, InstantiationException {
    Job job = Job.getInstance();
    FileInputFormat.setMinInputSplitSize(job, desiredBundleSizeBytes);
    FileInputFormat.setMaxInputSplitSize(job, desiredBundleSizeBytes);
    return createFormat(job).getSplits(job);
  }

  private FileInputFormat<K, V> createFormat(Job job)
      throws IOException, IllegalAccessException, InstantiationException {
    Path path = new Path(filepattern);
    FileInputFormat.addInputPath(job, path);
    return formatClass.newInstance();
  }

  private List<FileStatus> listStatus(FileInputFormat<K, V> format, Job job)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // FileInputFormat#listStatus is protected, so call using reflection
    Method listStatus = FileInputFormat.class.getDeclaredMethod("listStatus", JobContext.class);
    listStatus.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<FileStatus> stat = (List<FileStatus>) listStatus.invoke(format, job);
    return stat;
  }

  @SuppressWarnings("unchecked")
  private static <T> Coder<T> getDefaultCoder(Class<T> c) {
    if (Writable.class.isAssignableFrom(c)) {
      Class<? extends Writable> writableClass = (Class<? extends Writable>) c;
      return (Coder<T>) WritableCoder.of(writableClass);
    } else if (Void.class.equals(c)) {
      return (Coder<T>) VoidCoder.of();
    }
    // TODO: how to use registered coders here?
    throw new IllegalStateException("Cannot find coder for " + c);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> castClass(Class<?> aClass) {
    return (Class<T>) aClass;
  }

  // =======================================================================
  // BoundedReader
  // =======================================================================

  private static class HDFSFileReader<T, K, V> extends BoundedSource.BoundedReader<T> {

    private final HDFSFileSource<T, K, V> source;
    private final String filepattern;
    private final Class<? extends FileInputFormat<K, V>> formatClass;
    private final Job job;

    private List<InputSplit> splits;
    private ListIterator<InputSplit> splitsIterator;

    private Configuration conf;
    private FileInputFormat<?, ?> format;
    private TaskAttemptContext attemptContext;
    private RecordReader<K, V> currentReader;
    private KV<K, V> currentPair;

    HDFSFileReader(
        HDFSFileSource<T, K, V> source,
        String filepattern,
        Class<? extends FileInputFormat<K, V>> formatClass,
        SerializableSplit serializableSplit)
        throws IOException {
      this.source = source;
      this.filepattern = filepattern;
      this.formatClass = formatClass;
      this.job = Job.getInstance();

      if (source.serializableConfiguration != null) {
        for (Map.Entry<String, String> entry : source.serializableConfiguration.get()) {
          job.getConfiguration().set(entry.getKey(), entry.getValue());
        }
      }

      if (serializableSplit != null) {
        this.splits = ImmutableList.of(serializableSplit.getSplit());
        this.splitsIterator = splits.listIterator();
      }
    }

    @Override
    public boolean start() throws IOException {
      Path path = new Path(filepattern);
      FileInputFormat.addInputPath(job, path);

      conf = job.getConfiguration();
      try {
        format = formatClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException("Cannot instantiate file input format " + formatClass, e);
      }
      attemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());

      if (splitsIterator == null) {
        splits = format.getSplits(job);
        splitsIterator = splits.listIterator();
      }

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try {
        if (currentReader != null && currentReader.nextKeyValue()) {
          currentPair = nextPair();
          return true;
        } else {
          while (splitsIterator.hasNext()) {
            // advance the reader and see if it has records
            InputSplit nextSplit = splitsIterator.next();
            @SuppressWarnings("unchecked")
            RecordReader<K, V> reader =
                (RecordReader<K, V>) format.createRecordReader(nextSplit, attemptContext);
            if (currentReader != null) {
              currentReader.close();
            }
            currentReader = reader;
            currentReader.initialize(nextSplit, attemptContext);
            if (currentReader.nextKeyValue()) {
              currentPair = nextPair();
              return true;
            }
            currentReader.close();
            currentReader = null;
          }
          // either no next split or all readers were empty
          currentPair = null;
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (currentPair == null) {
        throw new NoSuchElementException();
      }
      return source.inputConverter.apply(currentPair);
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      currentPair = null;
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }

    @SuppressWarnings("unchecked")
    private KV<K, V> nextPair() throws IOException, InterruptedException {
      K key = currentReader.getCurrentKey();
      V value = currentReader.getCurrentValue();
      // clone Writable objects since they are reused between calls to RecordReader#nextKeyValue
      if (key instanceof Writable) {
        key = (K) WritableUtils.clone((Writable) key, conf);
      }
      if (value instanceof Writable) {
        value = (V) WritableUtils.clone((Writable) value, conf);
      }
      return KV.of(key, value);
    }

    // =======================================================================
    // Optional overrides
    // =======================================================================

    @Override
    public Double getFractionConsumed() {
      if (currentReader == null) {
        return 0.0;
      }
      if (splits.isEmpty()) {
        return 1.0;
      }
      int index = splitsIterator.previousIndex();
      int numReaders = splits.size();
      if (index == numReaders) {
        return 1.0;
      }
      double before = 1.0 * index / numReaders;
      double after = 1.0 * (index + 1) / numReaders;
      Double fractionOfCurrentReader = getProgress();
      if (fractionOfCurrentReader == null) {
        return before;
      }
      return before + fractionOfCurrentReader * (after - before);
    }

    private Double getProgress() {
      try {
        return (double) currentReader.getProgress();
      } catch (IOException | InterruptedException e) {
        return null;
      }
    }

  }

  // =======================================================================
  // SerializableSplit
  // =======================================================================

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be
   * serialized using Java's standard serialization mechanisms. Note that the InputSplit
   * has to be Writable (which most are).
   */
  private static class SerializableSplit implements Externalizable {
    private static final long serialVersionUID = 0L;

    private InputSplit split;

    public SerializableSplit() {
    }

    public SerializableSplit(InputSplit split) {
      checkArgument(split instanceof Writable, "Split is not writable: %s", split);
      this.split = split;
    }

    public InputSplit getSplit() {
      return split;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(split.getClass().getCanonicalName());
      ((Writable) split).write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String className = in.readUTF();
      try {
        split = (InputSplit) Class.forName(className).newInstance();
        ((Writable) split).readFields(in);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException(e);
      }
    }
  }

}
