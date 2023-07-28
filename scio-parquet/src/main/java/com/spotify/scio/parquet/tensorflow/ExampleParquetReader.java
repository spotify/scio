package com.spotify.scio.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.tensorflow.proto.example.Example;

import java.io.IOException;

public class ExampleParquetReader extends ParquetReader<Example> {

    ExampleParquetReader(Configuration conf, Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
        super(conf, file, new ExampleReadSupport(), unboundRecordFilter);
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
            return new ExampleReadSupport();
        }
    }
}
