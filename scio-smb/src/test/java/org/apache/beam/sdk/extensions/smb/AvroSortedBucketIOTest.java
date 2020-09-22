package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroSortedBucketIOTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testReadSerializable() {
    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString())));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString())));
  }

  @Test
  public void testTransformSerializable() {
    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.class)
                    .from(folder.toString()))
            .transform(
                AvroSortedBucketIO.transformOutput(String.class, "name", AvroGeneratedUser.class)
                    .to(folder.toString())));

    SerializableUtils.ensureSerializable(
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(new TupleTag<>("input"), AvroGeneratedUser.getClassSchema())
                    .from(folder.toString()))
            .transform(
                AvroSortedBucketIO.transformOutput(
                        String.class, "name", AvroGeneratedUser.getClassSchema())
                    .to(folder.toString())));
  }
}
