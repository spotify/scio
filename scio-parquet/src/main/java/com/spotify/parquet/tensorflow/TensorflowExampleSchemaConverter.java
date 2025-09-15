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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.FixedShape;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.metadata.v0.ValueCount;
import org.tensorflow.metadata.v0.ValueCountList;

public class TensorflowExampleSchemaConverter {

  public TensorflowExampleSchemaConverter(Configuration conf) {}

  public MessageType convert(Schema tfSchema) {
    return new MessageType("example", convertFeatures(tfSchema.getFeatureList()));
  }

  private List<Type> convertFeatures(List<Feature> features) {
    List<Type> types = new ArrayList<Type>();
    for (Feature feature : features) {
      if (feature.getType().equals(FeatureType.TYPE_UNKNOWN)) {
        continue;
      }
      types.add(convertFeature(feature));
    }
    return types;
  }

  private Type.Repetition repetitionShape(FixedShape shape) {
    if (shape.getDimCount() == 1 && shape.getDim(0).getSize() == 1) {
      return Type.Repetition.REQUIRED;
    } else {
      return Type.Repetition.REPEATED;
    }
  }

  private Type.Repetition repetitionValueCount(ValueCount valueCount) {
    long min = valueCount.getMin();
    long max = valueCount.getMax();
    if (min == 0 && max == 1) {
      return Type.Repetition.OPTIONAL;
    } else if (min == 1 && max == 1) {
      return Type.Repetition.REQUIRED;
    } else {
      return Type.Repetition.REPEATED;
    }
  }

  private Type.Repetition repetitionValueCounts(ValueCountList valueCounts) {
    if (valueCounts.getValueCountCount() == 1) {
      return repetitionValueCount(valueCounts.getValueCount(0));
    } else {
      return Type.Repetition.REPEATED;
    }
  }

  private Type convertFeature(Feature feature) {
    String name = feature.getName();
    Types.PrimitiveBuilder<PrimitiveType> builder;

    Type.Repetition repetition;
    if (feature.hasShape()) {
      repetition = repetitionShape(feature.getShape());
    } else if (feature.hasValueCount()) {
      repetition = repetitionValueCount(feature.getValueCount());
    } else {
      repetition = repetitionValueCounts(feature.getValueCounts());
    }

    FeatureType type = feature.getType();
    if (type.equals(FeatureType.INT)) {
      builder = Types.primitive(INT64, repetition);
    } else if (type.equals(FeatureType.FLOAT)) {
      builder = Types.primitive(FLOAT, repetition);
    } else if (type.equals(FeatureType.BYTES)) {
      builder = Types.primitive(BINARY, repetition);
    } else {
      throw new UnsupportedOperationException("Cannot convert tensorflow type " + type);
    }
    return builder.named(name);
  }

  public Schema convert(MessageType parquetSchema) {
    return Schema.newBuilder().addAllFeature(convertFields(parquetSchema.getFields())).build();
  }

  private List<Feature> convertFields(List<Type> parquetFields) {
    List<Feature> features = new ArrayList<>();
    for (Type parquetType : parquetFields) {
      Feature feature = convertField(parquetType);
      features.add(feature);
    }

    return features;
  }

  private ValueCount convertRepetition(Type.Repetition repetition) {
    switch (repetition) {
      case REQUIRED:
        return ValueCount.newBuilder().setMin(1).setMax(1).build();
      case OPTIONAL:
        return ValueCount.newBuilder().setMin(0).setMax(1).build();
      default:
        return null;
    }
  }

  private Feature convertField(final Type parquetType) {
    if (!parquetType.isPrimitive()) {
      throw new UnsupportedOperationException("Only primitive fields are supported");
    } else {
      final String featureName = parquetType.getName();
      final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
      final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
          asPrimitive.getPrimitiveTypeName();
      final Feature feature =
          parquetPrimitiveTypeName.convert(
              new PrimitiveType.PrimitiveTypeNameConverter<Feature, RuntimeException>() {
                @Override
                public Feature convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                  return Feature.newBuilder().setName(featureName).setType(FeatureType.INT).build();
                }

                @Override
                public Feature convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new UnsupportedOperationException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertFIXED_LEN_BYTE_ARRAY(
                    PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                  throw new UnsupportedOperationException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws UnsupportedOperationException {
                  throw new UnsupportedOperationException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                  return Feature.newBuilder()
                      .setName(featureName)
                      .setType(FeatureType.FLOAT)
                      .build();
                }

                @Override
                public Feature convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new UnsupportedOperationException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new UnsupportedOperationException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                  return Feature.newBuilder()
                      .setName(featureName)
                      .setType(FeatureType.BYTES)
                      .build();
                }
              });
      final ValueCount valueCount = convertRepetition(asPrimitive.getRepetition());
      return valueCount == null ? feature : feature.toBuilder().setValueCount(valueCount).build();
    }
  }
}
