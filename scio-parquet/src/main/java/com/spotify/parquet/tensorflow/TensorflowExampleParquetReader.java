/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.parquet.tensorflow;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.tensorflow.proto.example.Example;

public class TensorflowExampleParquetReader extends ParquetReader<Example> {

  TensorflowExampleParquetReader(
      Configuration conf, Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(conf, file, new TensorflowExampleReadSupport(), unboundRecordFilter);
  }

  public static Builder builder(InputFile file) {
    return new Builder(file);
  }

  public static class Builder extends ParquetReader.Builder<Example> {
    private Builder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<Example> getReadSupport() {
      return new TensorflowExampleReadSupport();
    }
  }
}
