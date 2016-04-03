package com.google.cloud.dataflow.contrib.hadoop.simpleauth;

import com.google.cloud.dataflow.contrib.hadoop.AvroHadoopFileSource;
import com.google.cloud.dataflow.contrib.hadoop.HadoopFileSource;
import com.google.cloud.dataflow.contrib.hadoop.HadoopUserUtils;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputSplit;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Source for Avros on Hadoop/HDFS with Simple Authentication.
 *
 * Allows to set arbitrary username as HDFS user, which is used for reading Avro from HDFS.
 */
public class SimpleAuthAvroHadoopFileSource<T> extends AvroHadoopFileSource<T>{
  // keep this field to pass Hadoop user between workers
  private final HadoopUserUtils user = new HadoopUserUtils();

  /**
   * Create a {@code SimpleAuthAvroHadoopFileSource} based on a file or a file pattern specification,
   * {@param username} is used for Simple Authentication with Hadoop.
   */
  public SimpleAuthAvroHadoopFileSource(String filepattern,
                                        AvroCoder<T> avroCoder,
                                        String username) {
    super(filepattern, avroCoder);
    user.setSimpleAuthUser(username);
  }

  /**
   * Create a {@code SimpleAuthAvroHadoopFileSource} based on a single Hadoop input split, which
   * won't be split up further, {@param username} is used for Simple Authentication with Hadoop.
   */
  public SimpleAuthAvroHadoopFileSource(String filepattern,
                                        AvroCoder<T> avroCoder,
                                        HadoopFileSource.SerializableSplit serializableSplit,
                                        String username) {
    super(filepattern, avroCoder, serializableSplit);
    user.setSimpleAuthUser(username);
  }

  @Override
  public List<? extends AvroHadoopFileSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, AvroHadoopFileSource<T>>() {
            @Nullable
            @Override
            public AvroHadoopFileSource<T> apply(@Nullable InputSplit inputSplit) {
              return new SimpleAuthAvroHadoopFileSource<>(filepattern,
                                                    avroCoder,
                                                    new HadoopFileSource.SerializableSplit(inputSplit),
                                                    user.getSimpleAuthUser());
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }
}
