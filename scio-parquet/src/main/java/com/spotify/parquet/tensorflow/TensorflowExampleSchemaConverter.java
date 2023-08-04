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
import org.tensorflow.metadata.v0.Schema;

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

  private Type convertFeature(Feature feature) {
    String name = feature.getName();
    Types.PrimitiveBuilder<PrimitiveType> builder;
    FeatureType type = feature.getType();
    if (type.equals(FeatureType.INT)) {
      builder = Types.primitive(INT64, Type.Repetition.REPEATED);
    } else if (type.equals(FeatureType.FLOAT)) {
      builder = Types.primitive(FLOAT, Type.Repetition.REPEATED);
    } else if (type.equals(FeatureType.BYTES)) {
      builder = Types.primitive(BINARY, Type.Repetition.REPEATED);
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

  private Feature convertField(final Type parquetType) {
    if (!parquetType.isPrimitive()) {
      throw new IllegalArgumentException("Only primitive fields are supported");
    } else {
      final String featureName = parquetType.getName();
      final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
      final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
          asPrimitive.getPrimitiveTypeName();
      Feature feature =
          parquetPrimitiveTypeName.convert(
              new PrimitiveType.PrimitiveTypeNameConverter<Feature, RuntimeException>() {
                @Override
                public Feature convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                  return Feature.newBuilder().setName(featureName).setType(FeatureType.INT).build();
                }

                @Override
                public Feature convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new IllegalArgumentException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertFIXED_LEN_BYTE_ARRAY(
                    PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                  throw new IllegalArgumentException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new IllegalArgumentException(
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
                  throw new IllegalArgumentException(
                      "Unsupported primitive type: " + primitiveTypeName);
                }

                @Override
                public Feature convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName)
                    throws RuntimeException {
                  throw new IllegalArgumentException(
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
      return feature;
    }
  }
}
