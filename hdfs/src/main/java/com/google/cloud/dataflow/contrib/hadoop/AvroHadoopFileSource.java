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

public class AvroHadoopFileSource<T> extends HadoopFileSource<AvroKey<T>, NullWritable> {

  private final AvroCoder<T> avroCoder;

  public AvroHadoopFileSource(String filepattern, AvroCoder<T> avroCoder) {
    this(filepattern, avroCoder, null);
  }

  public AvroHadoopFileSource(String filepattern, AvroCoder<T> avroCoder, SerializableSplit serializableSplit) {
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

      Coder<T> avroCoder = ((AvroHadoopFileSource<T>) this.getCurrentSource()).avroCoder;
      key = new AvroKey(CoderUtils.clone(avroCoder, key.datum()));

      return KV.of(key, value);
    }

  }

  private static class ClassUtil {
    @SuppressWarnings("unchecked")
    private static <T> Class<T> castClass(Class<?> aClass) {
      return (Class<T>)aClass;
    }
  }

}
