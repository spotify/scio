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
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** E2E test for heterogeneously-typed SMB join. */
public class MixedSourcesEndToEndTest {
  @Rule public final TestPipeline pipeline1 = TestPipeline.create();
  @Rule public final TestPipeline pipeline2 = TestPipeline.create();
  @Rule public final TestPipeline pipeline3 = TestPipeline.create();
  @Rule public final TemporaryFolder sourceFolder1 = new TemporaryFolder();
  @Rule public final TemporaryFolder sourceFolder2 = new TemporaryFolder();

  @Rule public final TemporaryFolder tmpFolder1 = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder2 = new TemporaryFolder();

  private static Comparator<GenericRecord> grComparator =
      Comparator.comparing(
              (GenericRecord o) ->
                  ByteString.copyFrom(((ByteBuffer) o.get("firstname")).duplicate()).toStringUtf8())
          .thenComparing(
              o -> ByteString.copyFrom(((ByteBuffer) o.get("lastname")).duplicate()).toStringUtf8())
          .thenComparingInt(o -> (Integer) o.get("age"));

  private static Comparator<TableRow> trComparator =
      Comparator.comparing((TableRow o) -> (String) o.get("firstname"))
          .thenComparing(o -> (String) o.get("lastname"))
          .thenComparing(o -> (String) o.get("country"));

  private static final Schema GR_USER_SCHEMA =
      Schema.createRecord(
          "user",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Arrays.asList(
              new Field(
                  "firstname",
                  Schema.createUnion(
                      Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.BYTES))),
                  "",
                  ""),
              new Field(
                  "lastname",
                  Schema.createUnion(
                      Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.BYTES))),
                  "",
                  ""),
              new Field("age", Schema.create(Type.INT), "", -1)));

  @SafeVarargs
  private static <A> List<A> list(A... a) {
    return Arrays.asList(a);
  }

  private static <A> List<A> empty() {
    return Collections.emptyList();
  }

  private static GenericRecord GR(String firstname, String lastname, int age) {
    GenericData.Record result = new GenericData.Record(GR_USER_SCHEMA);
    result.put(
        "firstname",
        Optional.ofNullable(firstname)
            .map(n -> ByteBuffer.wrap(ByteString.copyFromUtf8(n).toByteArray()))
            .orElse(null));
    result.put(
        "lastname",
        Optional.ofNullable(lastname)
            .map(n -> ByteBuffer.wrap(ByteString.copyFromUtf8(n).toByteArray()))
            .orElse(null));
    result.put("age", age);
    return result;
  }

  private static TableRow Json(String firstname, String lastname, String country) {
    return new TableRow()
        .set("firstname", firstname)
        .set("lastname", lastname)
        .set("country", country);
  }

  public <KeyType> void e2e(
      BiFunction<
              TupleTag<GenericRecord>,
              TupleTag<TableRow>,
              PTransform<PBegin, PCollection<KV<KeyType, CoGbkResult>>>>
          srcFun,
      Coder<KeyType> keyCoder,
      List<KV<KeyType, KV<List<GenericRecord>, List<TableRow>>>> expectedIn)
      throws Exception {
    List<KV<KeyType, KV<List<GenericRecord>, List<TableRow>>>> expected =
        expectedIn.stream()
            .map(
                kv ->
                    KV.of(
                        kv.getKey(),
                        KV.of(
                            kv.getValue().getKey().stream()
                                .sorted(grComparator)
                                .collect(Collectors.toList()),
                            kv.getValue().getValue().stream()
                                .sorted(trComparator)
                                .collect(Collectors.toList()))))
            .collect(Collectors.toList());

    pipeline1
        .apply(
            Create.of(
                    GR("a", "a", 1),
                    GR("a", "b", 1),
                    GR("a", "c", 1),
                    GR("a", "c", 2),
                    GR("b", "b", 2),
                    GR("c", "c", 3),
                    GR("d", "d", 4),
                    GR("e", "e", 5),
                    GR("f", "f", 6),
                    GR("g", "g", 7),
                    GR(null, null, 7),
                    GR("h", "h", 8))
                .withCoder(AvroCoder.of(GR_USER_SCHEMA)))
        .apply(
            AvroSortedBucketIO.write(
                    ByteBuffer.class, "firstname", ByteBuffer.class, "lastname", GR_USER_SCHEMA)
                .to(sourceFolder1.getRoot().getPath())
                .withTempDirectory(tmpFolder1.getRoot().getPath())
                .withNumBuckets(8)
                .withNumShards(4)
                .withHashType(HashType.MURMUR3_32)
                .withSuffix(".avro")
                .withCodec(CodecFactory.snappyCodec()));

    pipeline1.run().waitUntilFinish();

    pipeline2
        .apply(
            Create.of(
                    Json("a", "a", "US"),
                    Json("a", "b", "US"),
                    Json("a", "c", "US"),
                    Json("a", "c", "MX"),
                    Json("c", "c", "DE"),
                    Json("d", "d", "MX"),
                    Json("e", "e", "AU"),
                    Json("f", "f", "US"),
                    Json("g", "g", "SE"),
                    Json(null, null, "SE"),
                    Json("h", "h", "DE"),
                    Json("i", "i", "MX"))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            JsonSortedBucketIO.write(String.class, "firstname", String.class, "lastname")
                .to(sourceFolder2.getRoot().getPath())
                .withTempDirectory(tmpFolder2.getRoot().getPath())
                .withNumBuckets(8)
                .withNumShards(4)
                .withHashType(HashType.MURMUR3_32)
                .withSuffix(".json")
                .withCompression(Compression.UNCOMPRESSED));
    pipeline2.run().waitUntilFinish();

    TupleTag<GenericRecord> lhsTag = new TupleTag<>();
    TupleTag<TableRow> rhsTag = new TupleTag<>();

    final PCollection<KV<KeyType, KV<List<GenericRecord>, List<TableRow>>>> joined =
        pipeline3
            .apply(srcFun.apply(lhsTag, rhsTag))
            .apply(ParDo.of(new ExpandResult<>(lhsTag, rhsTag)))
            .setCoder(
                KvCoder.of(
                    keyCoder,
                    KvCoder.of(
                        ListCoder.of(AvroCoder.of(GR_USER_SCHEMA)),
                        ListCoder.of(TableRowJsonCoder.of()))));

    PAssert.that(joined).containsInAnyOrder(expected);
    pipeline3.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2E() throws Exception {
    e2e(
        (lhsTag, rhsTag) ->
            SortedBucketIO.read(String.class)
                .of(
                    AvroSortedBucketIO.read(lhsTag, GR_USER_SCHEMA)
                        .from(sourceFolder1.getRoot().getPath()))
                .and(JsonSortedBucketIO.read(rhsTag).from(sourceFolder2.getRoot().getPath())),
        StringUtf8Coder.of(),
        Arrays.asList(
            KV.of(
                "a",
                KV.of(
                    list(GR("a", "a", 1), GR("a", "b", 1), GR("a", "c", 1), GR("a", "c", 2)),
                    list(
                        Json("a", "a", "US"),
                        Json("a", "b", "US"),
                        Json("a", "c", "US"),
                        Json("a", "c", "MX")))),
            KV.of("b", KV.of(list(GR("b", "b", 2)), empty())),
            KV.of("c", KV.of(list(GR("c", "c", 3)), list(Json("c", "c", "DE")))),
            KV.of("d", KV.of(list(GR("d", "d", 4)), list(Json("d", "d", "MX")))),
            KV.of("e", KV.of(list(GR("e", "e", 5)), list(Json("e", "e", "AU")))),
            KV.of("f", KV.of(list(GR("f", "f", 6)), list(Json("f", "f", "US")))),
            KV.of("g", KV.of(list(GR("g", "g", 7)), list(Json("g", "g", "SE")))),
            KV.of("h", KV.of(list(GR("h", "h", 8)), list(Json("h", "h", "DE")))),
            KV.of("i", KV.of(empty(), list(Json("i", "i", "MX"))))));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2ESecondary() throws Exception {
    e2e(
        (lhsTag, rhsTag) ->
            SortedBucketIO.read(String.class, String.class)
                .of(
                    AvroSortedBucketIO.read(lhsTag, GR_USER_SCHEMA)
                        .from(sourceFolder1.getRoot().getPath()))
                .and(JsonSortedBucketIO.read(rhsTag).from(sourceFolder2.getRoot().getPath())),
        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
        Arrays.asList(
            KV.of(KV.of("a", "a"), KV.of(list(GR("a", "a", 1)), list(Json("a", "a", "US")))),
            KV.of(KV.of("a", "b"), KV.of(list(GR("a", "b", 1)), list(Json("a", "b", "US")))),
            KV.of(
                KV.of("a", "c"),
                KV.of(
                    list(GR("a", "c", 1), GR("a", "c", 2)),
                    list(Json("a", "c", "US"), Json("a", "c", "MX")))),
            KV.of(KV.of("b", "b"), KV.of(list(GR("b", "b", 2)), empty())),
            KV.of(KV.of("c", "c"), KV.of(list(GR("c", "c", 3)), list(Json("c", "c", "DE")))),
            KV.of(KV.of("d", "d"), KV.of(list(GR("d", "d", 4)), list(Json("d", "d", "MX")))),
            KV.of(KV.of("e", "e"), KV.of(list(GR("e", "e", 5)), list(Json("e", "e", "AU")))),
            KV.of(KV.of("f", "f"), KV.of(list(GR("f", "f", 6)), list(Json("f", "f", "US")))),
            KV.of(KV.of("g", "g"), KV.of(list(GR("g", "g", 7)), list(Json("g", "g", "SE")))),
            KV.of(KV.of("h", "h"), KV.of(list(GR("h", "h", 8)), list(Json("h", "h", "DE")))),
            KV.of(KV.of("i", "i"), KV.of(empty(), list(Json("i", "i", "MX"))))));
  }

  private static class ExpandResult<KeyType>
      extends DoFn<KV<KeyType, CoGbkResult>, KV<KeyType, KV<List<GenericRecord>, List<TableRow>>>> {
    private final TupleTag<GenericRecord> lhsTag;
    private final TupleTag<TableRow> rhsTag;

    private ExpandResult(TupleTag<GenericRecord> lhsTag, TupleTag<TableRow> rhsTag) {
      this.lhsTag = lhsTag;
      this.rhsTag = rhsTag;
    }

    @ProcessElement
    public void processElement(
        @Element KV<KeyType, CoGbkResult> kv,
        OutputReceiver<KV<KeyType, KV<List<GenericRecord>, List<TableRow>>>> out) {
      final CoGbkResult result = kv.getValue();
      final List<GenericRecord> genericRecords =
          Lists.newArrayList(result.getAll(lhsTag).iterator());
      genericRecords.sort(grComparator);
      final List<TableRow> jsons = Lists.newArrayList(result.getAll(rhsTag).iterator());
      jsons.sort(trComparator);
      out.output(KV.of(kv.getKey(), KV.of(genericRecords, jsons)));
    }
  }
}
