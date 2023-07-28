package com.spotify.scio.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

import java.io.IOException;

public class ExampleParquetWriter extends ParquetWriter<Example> {

    public static Builder builder(OutputFile file) {
        return new Builder(file);
    }

    ExampleParquetWriter(
            Path file,
            WriteSupport<Example> writeSupport,
            CompressionCodecName compressionCodecName,
            int blockSize, int pageSize, boolean enableDictionary,
            boolean enableValidation, ParquetProperties.WriterVersion writerVersion,
            Configuration conf
    ) throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    private static WriteSupport<Example> writeSupport(Configuration conf, Schema schema) {
        return new ExampleWriteSupport(new ExampleSchemaConverter(conf).convert("", schema), schema);
    }

    public static class Builder extends ParquetWriter.Builder<Example, Builder> {
        private Schema schema;

        protected Builder(OutputFile file) {
            super(file);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        @Override
        protected WriteSupport<Example> getWriteSupport(Configuration conf) {
            return ExampleParquetWriter.writeSupport(conf, schema);
        }
    }
}
