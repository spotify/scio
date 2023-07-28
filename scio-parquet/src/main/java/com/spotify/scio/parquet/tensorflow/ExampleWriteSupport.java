package com.spotify.scio.parquet.tensorflow;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.Example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ExampleWriteSupport extends WriteSupport<Example> {

    private RecordConsumer recordConsumer;
    private String name;
    private MessageType rootSchema;
    private Schema rootTFSchema;

    static final String EXAMPLE_SCHEMA = "parquet.example.schema";
    static final String EXAMPLE_NAME = "parquet.example.name";

    public ExampleWriteSupport(MessageType schema, Schema tfSchema) {
        this.rootSchema = schema;
        this.rootTFSchema = tfSchema;
    }

    @Override
    public String getName() {
        return "example";
    }

    @Override
    public WriteContext init(Configuration configuration) {
        if (rootTFSchema == null) {
            try {
                this.name = configuration.get(configuration.get(EXAMPLE_NAME));
                this.rootTFSchema = TextFormat.parse(configuration.get(EXAMPLE_SCHEMA), Schema.class);
                this.rootSchema = new ExampleSchemaConverter(configuration).convert(name, rootTFSchema);
            } catch (TextFormat.ParseException e) {
                throw new RuntimeException(e);
            }
        }

        Map<String, String> extraMetaData = new HashMap<String, String>();
        extraMetaData.put(ExampleReadSupport.EXAMPLE_NAME_METADATA_KEY, name);
        extraMetaData.put(ExampleReadSupport.EXAMPLE_SCHEMA_METADATA_KEY, TextFormat.printer().printToString(rootTFSchema));
        return new WriteContext(rootSchema, extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Example record) {
        recordConsumer.startMessage();
        writeRecordFields(rootSchema, rootTFSchema, record);
        recordConsumer.endMessage();
    }

    private void writeRecordFields(GroupType schema, Schema tfSchema, Example example) {
        List<Type> fields = schema.getFields();
        List<Feature> mdFeatures = tfSchema.getFeatureList();
        for (int index = 0; index < mdFeatures.size(); index++) {
            Feature mdFeature = mdFeatures.get(index);
            FeatureType type = mdFeature.getType();
            org.tensorflow.proto.example.Feature value = example.getFeatures().getFeatureOrDefault(
                    mdFeature.getName(),
                    org.tensorflow.proto.example.Feature.getDefaultInstance()
            );
            Type fieldType = fields.get(index);
            String fieldName = fieldType.getName();
            switch (type) {
                case BYTES:
                    List<Binary> bytes = value
                            .getBytesList()
                            .getValueList()
                            .stream()
                            .map(ByteString::toByteArray)
                            .map(Binary::fromReusedByteArray)
                            .collect(Collectors.toList());
                    writeField(fieldName, index, bytes, recordConsumer::addBinary);
                    break;
                case INT:
                    List<Long> longs = value.getInt64List().getValueList();
                    writeField(fieldName, index, longs, recordConsumer::addLong);
                    break;
                case FLOAT:
                    List<Float> floats = value.getFloatList().getValueList();
                    writeField(fieldName, index, floats, recordConsumer::addFloat);
                    break;
                default:
                    throw new IllegalArgumentException("Cannot write " + type);
            }
        }
    }

    private <T> void writeField(String name, int index, List<T> values, Consumer<T> add) {
        if (!values.isEmpty()) {
            recordConsumer.startField(name, index);
            values.forEach(add);
            recordConsumer.endField(name, index);
        }
    }
}
