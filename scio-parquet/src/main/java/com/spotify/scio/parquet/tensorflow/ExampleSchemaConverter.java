package com.spotify.scio.parquet.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.tensorflow.metadata.v0.Annotation;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

public class ExampleSchemaConverter {

    public ExampleSchemaConverter(Configuration conf) {

    }

    public MessageType convert(String name, Schema tfSchema) {
        return new MessageType(name, convertFeatures(tfSchema.getFeatureList()));
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
        return convertFields(parquetSchema.getFields());
    }

    private Schema convertFields(List<Type> parquetFields) {
        List<Feature> features = new ArrayList<Feature>();
        for (Type parquetType : parquetFields) {
            Feature feature = convertField(parquetType);
            features.add(feature);
        }

        Schema schema = Schema
                .newBuilder()
                .addAllFeature(features)
                .setAnnotation(Annotation.newBuilder().build()) // TODO
                .build();
        return schema;
    }

    private Feature convertField(final Type parquetType) {
        if (!parquetType.isPrimitive()) {
            throw new IllegalArgumentException("Only primitive fields are supported");
        } else {
            final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
            final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
                    asPrimitive.getPrimitiveTypeName();
            Feature feature = parquetPrimitiveTypeName.convert(
                    new PrimitiveType.PrimitiveTypeNameConverter<Feature, RuntimeException>() {
                        @Override
                        public Feature convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                            return Feature.newBuilder().setType(FeatureType.INT).build();
                        }

                        @Override
                        public Feature convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName);
                        }

                        @Override
                        public Feature convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName);
                        }

                        @Override
                        public Feature convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName);
                        }

                        @Override
                        public Feature convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                            return Feature.newBuilder().setType(FeatureType.FLOAT).build();
                        }

                        @Override
                        public Feature convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName);
                        }

                        @Override
                        public Feature convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName);
                        }

                        @Override
                        public Feature convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
                            return Feature.newBuilder().setType(FeatureType.BYTES).build();
                        }
                    });
            return feature;
        }
    }

}
