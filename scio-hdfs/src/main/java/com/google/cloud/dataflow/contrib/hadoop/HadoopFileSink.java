/*
 * Copyright (C) 2015 The Google Cloud Dataflow Hadoop Library Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.hadoop;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A {@code Sink} for writing records to a Hadoop filesystem using a Hadoop file-based output format.
 *
 * @param <K> The type of keys to be written to the sink.
 * @param <V> The type of values to be written to the sink.
 */
public class HadoopFileSink<K, V> extends Sink<KV<K, V>> {
  protected static final String jtIdentifier = "scio_job";

  protected final String path;
  protected final Class<? extends FileOutputFormat<K, V>> formatClass;

  // workaround to make Configuration serializable
  private final Map<String, String> map;

  public HadoopFileSink(String path, Class<? extends FileOutputFormat<K, V>> formatClass) {
    this.path = path;
    this.formatClass = formatClass;
    this.map = Maps.newHashMap();
  }

  public HadoopFileSink(String path, Class<? extends FileOutputFormat<K, V>> formatClass, Configuration conf) {
    this(path, formatClass);
    // serialize conf to map
    for (Map.Entry<String, String> entry : conf) {
      map.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void validate(PipelineOptions options) {
    try {
      Job job = jobInstance();
      FileSystem fs = FileSystem.get(job.getConfiguration());
      Preconditions.checkState(!fs.exists(new Path(path)), "Output path " + path + " already exists");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public WriteOperation<KV<K, V>, ?> createWriteOperation(PipelineOptions options) {
    return new HadoopWriteOperation<>(this, path, formatClass);
  }

  protected Job jobInstance() throws IOException {
    Job job = Job.getInstance();
    // deserialize map to conf
    Configuration conf = job.getConfiguration();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return job;
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  public static class HadoopWriteOperation<K, V> extends WriteOperation<KV<K, V>, String> {
    private final Sink<KV<K, V>> sink;
    protected final String path;
    protected final Class<? extends FileOutputFormat<K, V>> formatClass;

    // unique job ID for this sink
    protected final int jobId;

    public HadoopWriteOperation(Sink<KV<K, V>> sink,
                                String path,
                                Class<? extends FileOutputFormat<K, V>> formatClass) {
      this.sink = sink;
      this.path = path;
      this.formatClass = formatClass;
      this.jobId = (int) (System.currentTimeMillis() / 1000);
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {
      Job job = ((HadoopFileSink<K, V>) getSink()).jobInstance();
      FileOutputFormat.setOutputPath(job, new Path(path));
    }

    @Override
    public void finalize(Iterable<String> writerResults, PipelineOptions options) throws Exception {
      // job successful
      Job job = ((HadoopFileSink<K, V>) getSink()).jobInstance();
      JobContext context = new JobContextImpl(job.getConfiguration(), jobID());
      FileOutputCommitter outputCommitter = new FileOutputCommitter(new Path(path), context);
      outputCommitter.commitJob(context);

      // get actual output shards
      Set<String> actual = Sets.newHashSet();
      FileSystem fs = FileSystem.get(job.getConfiguration());
      FileStatus[] statuses = fs.listStatus(new Path(path), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String name = path.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });

      // get expected output shards
      Set<String> expected = Sets.newHashSet(writerResults);
      for (FileStatus s : statuses) {
        String name = s.getPath().getName();
        int pos = name.indexOf('.');
        actual.add(pos > 0 ? name.substring(0, pos) : name);
      }

      Preconditions.checkState(actual.equals(expected), "Writer results and output files do not match");

      // rename output shards to Hadoop style, i.e. part-r-00000.txt
      int i = 0;
      for (FileStatus s : statuses) {
        String name = s.getPath().getName();
        int pos = name.indexOf('.');
        String ext = pos > 0 ? name.substring(pos) : "";
        fs.rename(s.getPath(), new Path(s.getPath().getParent(), String.format("part-r-%05d%s", i, ext)));
        i++;
      }
    }

    @Override
    public Writer<KV<K, V>, String> createWriter(PipelineOptions options) throws Exception {
      return new HadoopWriter<>(this, path, formatClass);
    }

    @Override
    public Sink<KV<K, V>> getSink() {
      return sink;
    }

    @Override
    public Coder<String> getWriterResultCoder() {
      return StringUtf8Coder.of();
    }

    private JobID jobID() {
      return new JobID(jtIdentifier, jobId);
    }

  }

  // =======================================================================
  // Writer
  // =======================================================================

  public static class HadoopWriter<K, V> extends Writer<KV<K, V>, String> {
    private final HadoopWriteOperation<K, V> writeOperation;
    private final String path;
    private final Class<? extends FileOutputFormat<K, V>> formatClass;

    // unique hash for each task
    private int hash;

    private TaskAttemptContext context;
    private RecordWriter<K, V> recordWriter;
    private FileOutputCommitter outputCommitter;

    public HadoopWriter(HadoopWriteOperation<K, V> writeOperation,
                        String path,
                        Class<? extends FileOutputFormat<K, V>> formatClass) {
      this.writeOperation = writeOperation;
      this.path = path;
      this.formatClass = formatClass;
    }

    @Override
    public void open(String uId) throws Exception {
      this.hash = uId.hashCode();

      Job job = ((HadoopFileSink<K, V>) getWriteOperation().getSink()).jobInstance();
      FileOutputFormat.setOutputPath(job, new Path(path));

      // Each Writer is responsible for writing one bundle of elements and is represented by one
      // unique Hadoop task based on uId/hash. All tasks share the same job ID. Since Dataflow
      // handles retrying of failed bundles, each task has one attempt only.
      JobID jobId = ((HadoopWriteOperation) writeOperation).jobID();
      TaskID taskId = new TaskID(jobId, TaskType.REDUCE, hash);
      context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(taskId, 0));

      FileOutputFormat<K, V> outputFormat = formatClass.newInstance();
      recordWriter = outputFormat.getRecordWriter(context);
      outputCommitter = (FileOutputCommitter) outputFormat.getOutputCommitter(context);
    }

    @Override
    public void write(KV<K, V> value) throws Exception {
      recordWriter.write(value.getKey(), value.getValue());
    }

    @Override
    public String close() throws Exception {
      // task/attempt successful
      recordWriter.close(context);
      outputCommitter.commitTask(context);

      // result is prefix of the output file name
      return String.format("part-r-%d", hash);
    }

    @Override
    public WriteOperation<KV<K, V>, String> getWriteOperation() {
      return writeOperation;
    }

  }

}
