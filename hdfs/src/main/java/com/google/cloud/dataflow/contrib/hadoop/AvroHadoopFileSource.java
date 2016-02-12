/*
 * Copyright (C) 2015 The Google Cloud Dataflow Hadoop Library Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * A {@code BoundedSource} for reading Avro files resident in a Hadoop filesystem.
 *
 * @param <T> The type of the Avro records to be read from the source.
 */
public class AvroHadoopFileSource<T> extends HadoopFileSource<AvroKey<T>, NullWritable> {

  private final AvroCoder<T> avroCoder;

  public AvroHadoopFileSource(String filepattern, AvroCoder<T> avroCoder) {
    this(filepattern, avroCoder, null);
  }

  public AvroHadoopFileSource(String filepattern, AvroCoder<T> avroCoder, HadoopFileSource.SerializableSplit serializableSplit) {
    super(filepattern,
        ClassUtil.<AvroKeyInputFormat<T>>castClass(AvroKeyInputFormat.class),
        ClassUtil.<AvroKey<T>>castClass(AvroKey.class),
        NullWritable.class, serializableSplit);
    this.avroCoder = avroCoder;
  }

  @Override
  public Coder<KV<AvroKey<T>, NullWritable>> getDefaultOutputCoder() {
    AvroWrapperCoder<AvroKey<T>, T> keyCoder = AvroWrapperCoder.of(this.getKeyClass(), avroCoder);
    return KvCoder.of(keyCoder, WritableCoder.of(NullWritable.class));
  }

  @Override
  public BoundedReader<KV<AvroKey<T>, NullWritable>> createReader(PipelineOptions options) throws IOException {
    this.validate();

    if (serializableSplit == null) {
      return new AvroHadoopFileReader<>(this, filepattern, formatClass);
    } else {
      return new AvroHadoopFileReader<>(this, filepattern, formatClass,
          serializableSplit.getSplit());
    }
  }

  static class AvroHadoopFileReader<T> extends HadoopFileReader<AvroKey<T>, NullWritable> {

    public AvroHadoopFileReader(BoundedSource<KV<AvroKey<T>, NullWritable>> source,
                                String filepattern, Class<? extends FileInputFormat<?, ?>> formatClass) {
      super(source, filepattern, formatClass);
    }

    public AvroHadoopFileReader(BoundedSource<KV<AvroKey<T>, NullWritable>> source,
                                String filepattern, Class<? extends FileInputFormat<?, ?>> formatClass,
                                InputSplit split) {
      super(source, filepattern, formatClass, split);
    }


    @SuppressWarnings("unchecked")
    protected KV<AvroKey<T>, NullWritable> nextPair() throws IOException, InterruptedException {
      AvroKey<T> key = currentReader.getCurrentKey();
      NullWritable value = currentReader.getCurrentValue();

      // clone the record to work around identical element issue due to object reuse
      Coder<T> avroCoder = ((AvroHadoopFileSource<T>) this.getCurrentSource()).avroCoder;
      key = new AvroKey(CoderUtils.clone(avroCoder, key.datum()));

      return KV.of(key, value);
    }

  }

  static class ClassUtil {
    @SuppressWarnings("unchecked")
    static <T> Class<T> castClass(Class<?> aClass) {
      return (Class<T>)aClass;
    }
  }

}
