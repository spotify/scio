package com.spotify.parquet.tensorflow;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.FixedShape;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.metadata.v0.ValueCount;
import org.tensorflow.metadata.v0.ValueCountList;

public class TensorflowExampleSchemaConverterTest {
  @Test
  public void testTensorflowTypeConversion() {
    Schema tfSchema =
        Schema.newBuilder()
            .addFeature(Feature.newBuilder().setName("int").setType(FeatureType.INT).build())
            .addFeature(Feature.newBuilder().setName("float").setType(FeatureType.FLOAT).build())
            .addFeature(Feature.newBuilder().setName("bytes").setType(FeatureType.BYTES).build())
            .build();
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());

    MessageType parquetSchema = converter.convert(tfSchema);
    Map<String, ColumnDescriptor> columns =
        parquetSchema.getColumns().stream()
            .collect(Collectors.toMap(cd -> String.join(".", cd.getPath()), Function.identity()));

    assertEquals(
        columns.get("int").getPrimitiveType().getPrimitiveTypeName(), PrimitiveTypeName.INT64);
    assertEquals(
        columns.get("float").getPrimitiveType().getPrimitiveTypeName(), PrimitiveTypeName.FLOAT);
    assertEquals(
        columns.get("bytes").getPrimitiveType().getPrimitiveTypeName(), PrimitiveTypeName.BINARY);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidTensorflowTypeConversion() {
    Schema tfSchema =
        Schema.newBuilder()
            .addFeature(Feature.newBuilder().setName("struct").setType(FeatureType.STRUCT).build())
            .build();
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());
    converter.convert(tfSchema);
  }

  @Test
  public void testParquetTypeConversion() {
    Type intField = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "int");
    Type floatField = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.FLOAT, "float");
    Type binaryField = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.BINARY, "bytes");
    MessageType parquetSchema = new MessageType("root", intField, floatField, binaryField);
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());

    Schema tfSchema = converter.convert(parquetSchema);
    Map<String, Feature> features =
        tfSchema.getFeatureList().stream()
            .collect(Collectors.toMap(Feature::getName, Function.identity()));

    assertEquals(features.get("int").getType(), FeatureType.INT);
    assertEquals(features.get("float").getType(), FeatureType.FLOAT);
    assertEquals(features.get("bytes").getType(), FeatureType.BYTES);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidParquetTypeConversion() {
    Type boolField = new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.BOOLEAN, "boolean");
    MessageType parquetSchema = new MessageType("root", boolField);
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());
    converter.convert(parquetSchema);
  }

  @Test
  public void testTensorflowRepetitionConversion() {
    Feature baseFeature = Feature.newBuilder().setType(FeatureType.INT).build();
    FixedShape requiredShape =
        FixedShape.newBuilder().addDim(FixedShape.Dim.newBuilder().setSize(1).build()).build();
    ValueCount requiredValueCount = ValueCount.newBuilder().setMin(1).setMax(1).build();
    ValueCountList requiredValueCounts =
        ValueCountList.newBuilder().addValueCount(requiredValueCount).build();
    ValueCount optionalValueCount = ValueCount.newBuilder().setMin(0).setMax(1).build();
    FixedShape repeatedShape =
        FixedShape.newBuilder()
            .addDim(FixedShape.Dim.newBuilder().setSize(3).build())
            .addDim(FixedShape.Dim.newBuilder().setSize(2))
            .build();
    ValueCount repeatedValueCount = ValueCount.newBuilder().setMin(1).setMax(4).build();
    ValueCountList repeatedValueCounts =
        ValueCountList.newBuilder()
            .addValueCount(requiredValueCount)
            .addValueCount(repeatedValueCount)
            .build();
    Schema tfSchema =
        Schema.newBuilder()
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("required_shape")
                    .setShape(requiredShape)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("required_value_count")
                    .setValueCount(requiredValueCount)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("required_value_counts")
                    .setValueCounts(requiredValueCounts)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("optional_value_count")
                    .setValueCount(optionalValueCount)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("repeated_shape")
                    .setShape(repeatedShape)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("repeated_value_count")
                    .setValueCount(repeatedValueCount)
                    .build())
            .addFeature(
                Feature.newBuilder(baseFeature)
                    .setName("repeated_value_counts")
                    .setValueCounts(repeatedValueCounts)
                    .build())
            .addFeature(Feature.newBuilder(baseFeature).setName("empty").build())
            .build();
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());

    MessageType parquetSchema = converter.convert(tfSchema);
    Map<String, ColumnDescriptor> columns =
        parquetSchema.getColumns().stream()
            .collect(Collectors.toMap(cd -> String.join(".", cd.getPath()), Function.identity()));

    assertEquals(
        columns.get("required_shape").getPrimitiveType().getRepetition(), Repetition.REQUIRED);
    assertEquals(
        columns.get("required_value_count").getPrimitiveType().getRepetition(),
        Repetition.REQUIRED);
    assertEquals(
        columns.get("required_value_counts").getPrimitiveType().getRepetition(),
        Repetition.REQUIRED);
    assertEquals(
        columns.get("required_value_counts").getPrimitiveType().getRepetition(),
        Repetition.REQUIRED);
    assertEquals(
        columns.get("optional_value_count").getPrimitiveType().getRepetition(),
        Repetition.OPTIONAL);
    assertEquals(
        columns.get("repeated_shape").getPrimitiveType().getRepetition(), Repetition.REPEATED);
    assertEquals(
        columns.get("repeated_value_count").getPrimitiveType().getRepetition(),
        Repetition.REPEATED);
    assertEquals(
        columns.get("repeated_value_counts").getPrimitiveType().getRepetition(),
        Repetition.REPEATED);
    assertEquals(columns.get("empty").getPrimitiveType().getRepetition(), Repetition.REPEATED);
  }

  @Test
  public void testParquetRepetitionConversion() {
    Type requiredField =
        new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "required");
    Type optionalField =
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "optional");
    Type repeatedField =
        new PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "repeated");
    MessageType parquetSchema =
        new MessageType("root", requiredField, optionalField, repeatedField);
    TensorflowExampleSchemaConverter converter =
        new TensorflowExampleSchemaConverter(new Configuration());

    Schema tfSchema = converter.convert(parquetSchema);
    Map<String, Feature> features =
        tfSchema.getFeatureList().stream()
            .collect(Collectors.toMap(Feature::getName, Function.identity()));

    assertEquals(
        features.get("required").getValueCount(),
        ValueCount.newBuilder().setMin(1).setMax(1).build());
    assertEquals(
        features.get("optional").getValueCount(),
        ValueCount.newBuilder().setMin(0).setMax(1).build());
    assertEquals(features.get("repeated").getShape(), FixedShape.newBuilder().build());
  }
}
