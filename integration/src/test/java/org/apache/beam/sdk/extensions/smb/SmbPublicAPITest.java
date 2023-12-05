/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Collections;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.PrimaryKeyedBucketedInput;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.TupleTag;
import org.tensorflow.proto.example.Example;

/** Test public API access level. Passes by successfully compiling. */
public class SmbPublicAPITest {
  public static void main(String[] args)
      throws Coder.NonDeterministicException, CannotProvideCoderException, IOException {
    // public API
    AvroSortedBucketIO.write(String.class, "name", AvroGeneratedUser.class).to("avro");
    JsonSortedBucketIO.write(String.class, "name").to("json");
    TensorFlowBucketIO.write(String.class, "name").to("tf");

    TupleTag<AvroGeneratedUser> avro = new TupleTag<>("avro");
    TupleTag<TableRow> json = new TupleTag<>("json");
    TupleTag<Example> tf = new TupleTag<>("tf");
    SortedBucketIO.read(String.class)
        .of(AvroSortedBucketIO.read(avro, AvroGeneratedUser.class).from("avro"))
        .and(JsonSortedBucketIO.read(json).from("json"))
        .and(TensorFlowBucketIO.read(tf).from("tf"));

    // extendable API
    new SortedBucketSink<>(
        new MyMetadata(8, 1, String.class, HashType.MURMUR3_32),
        FileSystems.matchNewResource("output", true),
        FileSystems.matchNewResource("temp", true),
        ".avro",
        new MyFileOperation(),
        1);

    new SortedBucketPrimaryKeyedSource<>(
        String.class,
        Collections.singletonList(
            new PrimaryKeyedBucketedInput<String>(
                new TupleTag<>(),
                Collections.singletonList("in"),
                ".avro",
                new MyFileOperation(),
                null)),
        TargetParallelism.auto(),
        null);
  }

  private static class MyMetadata extends BucketMetadata<String, Void, String> {
    private MyMetadata(
        int numBuckets, int numShards, Class<String> keyClass, BucketMetadata.HashType hashType)
        throws CannotProvideCoderException, Coder.NonDeterministicException {
      super(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, keyClass, hashType);
    }

    @Override
    public boolean isPartitionCompatibleForPrimaryKey(BucketMetadata other) {
      return true;
    }

    @Override
    public boolean isPartitionCompatibleForPrimaryAndSecondaryKey(BucketMetadata other) {
      return false;
    }

    @Override
    public String extractKeyPrimary(final String value) {
      return null;
    }

    @Override
    public Void extractKeySecondary(final String value) {
      return null;
    }
  }

  private static class MyFileOperation extends FileOperations<String> {

    private MyFileOperation() {
      super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    }

    @Override
    protected Reader<String> createReader() {
      return null;
    }

    @Override
    protected FileIO.Sink<String> createSink() {
      return null;
    }

    @Override
    public Coder<String> getCoder() {
      return null;
    }
  }
}
