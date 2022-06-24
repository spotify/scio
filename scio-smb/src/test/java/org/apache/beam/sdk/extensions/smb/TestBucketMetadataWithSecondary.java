package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;

public class TestBucketMetadataWithSecondary extends BucketMetadata<String, String, String> {
  @JsonProperty("keyIndex")
  private Integer keyIndex = 0;

  @JsonProperty("keyIndexSecondary")
  private Integer keyIndexSecondary = 1;

  static TestBucketMetadataWithSecondary of(int numBuckets, int numShards) {
    return of(numBuckets, numShards, SortedBucketIO.DEFAULT_FILENAME_PREFIX);
  }

  static TestBucketMetadataWithSecondary of(int numBuckets, int numShards, String filenamePrefix) {
    try {
      return new TestBucketMetadataWithSecondary(
          numBuckets, numShards, BucketMetadata.HashType.MURMUR3_32, filenamePrefix);
    } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
      throw new RuntimeException(e);
    }
  }

  TestBucketMetadataWithSecondary(
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("filenamePrefix") String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, hashType, filenamePrefix);
  }

  @JsonCreator
  TestBucketMetadataWithSecondary(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("filenamePrefix") String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    super(version, numBuckets, numShards, String.class, String.class, hashType, filenamePrefix);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TestBucketMetadataWithSecondary metadata = (TestBucketMetadataWithSecondary) o;
    return this.keyIndex.equals(metadata.keyIndex)
        && this.getNumBuckets() == metadata.getNumBuckets()
        && this.getNumShards() == metadata.getNumShards()
        && this.getHashType() == metadata.getHashType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyIndex, getNumBuckets(), getNumShards(), getHashType());
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryKey(final BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    TestBucketMetadataWithSecondary that = (TestBucketMetadataWithSecondary) o;
    return keyIndex.equals(that.keyIndex);
  }

  @Override
  public boolean isPartitionCompatibleForPrimaryAndSecondaryKey(final BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) return false;
    TestBucketMetadataWithSecondary that = (TestBucketMetadataWithSecondary) o;
    return keyIndex.equals(that.keyIndex) && keyIndexSecondary.equals(that.keyIndexSecondary);
  }

  @Override
  public String extractKeyPrimary(final String value) {
    try {
      return value.substring(keyIndex, keyIndex + 1);
    } catch (StringIndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public String extractKeySecondary(final String value) {
    try {
      return value.substring(keyIndexSecondary, keyIndexSecondary + 1);
    } catch (StringIndexOutOfBoundsException e) {
      return null;
    }
  }
}
