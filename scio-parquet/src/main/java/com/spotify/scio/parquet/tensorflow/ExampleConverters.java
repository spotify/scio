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

package com.spotify.scio.parquet.tensorflow;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.Schema;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Example;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.Features;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

class ExampleConverters {
  static class ExampleConverter extends GroupConverter {
    private final String[] names;
    private final FeatureConverter[] converters;

    private final Features.Builder builder = Features.newBuilder();

    public ExampleConverter(GroupType parquetSchema, Schema tfSchema) {
      this.names = new String[parquetSchema.getFieldCount()];
      this.converters = new FeatureConverter[parquetSchema.getFieldCount()];

      Map<String, FeatureType> featureTypes =
          tfSchema.getFeatureList().stream()
              .collect(
                  Collectors.toMap(
                      org.tensorflow.metadata.v0.Feature::getName,
                      org.tensorflow.metadata.v0.Feature::getType));
      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
        String featureName = parquetSchema.getFieldName(i);
        FeatureType type = featureTypes.get(featureName);
        names[i] = featureName;
        switch (type) {
          case INT:
            converters[i] = new IntConverter();
            break;
          case FLOAT:
            converters[i] = new FloatConverter();
            break;
          case BYTES:
            converters[i] = new BytesConverter();
            break;
          default:
            throw new IllegalArgumentException("Unsupported feature type: " + type);
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      builder.clear();
    }

    @Override
    public void end() {
      for (int i = 0; i < names.length; i++) {
        Feature feature = converters[i].get();
        if (feature != null) {
          builder.putFeature(names[i], feature);
        }
      }
    }

    public Example get() {
      Example example = Example.newBuilder().setFeatures(builder.build()).build();
      builder.clear();
      return example;
    }
  }

  abstract static class FeatureConverter extends PrimitiveConverter {
    public abstract Feature get();
  }

  static class IntConverter extends FeatureConverter {
    private final Int64List.Builder builder = Int64List.newBuilder();

    @Override
    public void addLong(long value) {
      builder.addValue(value);
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      Feature feature = n == 0 ? null : Feature.newBuilder().setInt64List(builder).build();
      builder.clear();
      return feature;
    }
  }

  static class FloatConverter extends FeatureConverter {
    private final FloatList.Builder builder = FloatList.newBuilder();

    @Override
    public void addFloat(float value) {
      builder.addValue(value);
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      Feature feature = n == 0 ? null : Feature.newBuilder().setFloatList(builder).build();
      builder.clear();
      return feature;
    }
  }

  static class BytesConverter extends FeatureConverter {
    private final BytesList.Builder builder = BytesList.newBuilder();

    @Override
    public void addBinary(Binary value) {
      builder.addValue(ByteString.copyFrom(value.getBytes()));
    }

    @Override
    public Feature get() {
      int n = builder.getValueCount();
      Feature feature = n == 0 ? null : Feature.newBuilder().setBytesList(builder).build();
      builder.clear();
      return feature;
    }
  }
}
