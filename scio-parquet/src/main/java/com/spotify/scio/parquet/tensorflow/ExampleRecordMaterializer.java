package com.spotify.scio.parquet.tensorflow;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

public class ExampleRecordMaterializer extends RecordMaterializer<Example> {

    private ExampleConverters.ExampleConverter root;

    public ExampleRecordMaterializer(MessageType requestedSchema, Schema tfSchema) {
        this.root = new ExampleConverters.ExampleConverter(requestedSchema, tfSchema);
    }

    @Override
    public Example getCurrentRecord() {
        return root.get();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
