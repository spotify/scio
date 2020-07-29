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

package com.spotify.scio.tensorflow

import com.google.protobuf.ByteString
import org.tensorflow.example._

import scala.jdk.CollectionConverters._

object MetadataSchemaTest {
  // Keep byte list the same length across examples to be parsed as a fixed shape.
  val e1Features: Map[String, Feature] = Map[String, Feature](
    "long" -> longFeature(Seq(1, 2, 3)),
    "bytes" -> byteStrFeature(Seq("a", "b", "c").map(ByteString.copyFromUtf8)),
    "floats" -> floatFeature(Seq(1.0f, 2.0f, 3.0f)),
    "indices" -> longFeature(Seq(1, 9)),
    "values" -> byteStrFeature(Seq("one", "nine").map(ByteString.copyFromUtf8)),
    "dense_shape" -> longFeature(Seq(100)),
    "missing_feature" -> longFeature(Seq(10))
  )
  val e2Features: Map[String, Feature] = Map[String, Feature](
    "long" -> longFeature(Seq(6)),
    "bytes" -> byteStrFeature(Seq("d", "e", "f").map(ByteString.copyFromUtf8)),
    "floats" -> floatFeature(Seq(4.0f, 5.0f)),
    "indices" -> longFeature(Seq(1, 2, 80)),
    "values" -> byteStrFeature(Seq("one", "two", "eighty").map(ByteString.copyFromUtf8)),
    "dense_shape" -> longFeature(Seq(100))
  )

  val e1FeatureList: Map[String, FeatureList] = Map[String, FeatureList](
    "string_list" -> featureList(
      Seq("one", "two", "eighty")
        .map(v => Seq(ByteString.copyFromUtf8(v)))
        .map(byteStrFeature)
    ),
    "long_list" -> featureList(Seq(1L, 2L, 3L).map(Seq(_)).map(longFeature)),
    "floats_list" -> featureList(Seq(1.0f, 2.0f, 3.0f).map(Seq(_)).map(floatFeature))
  )

  val examples: Seq[Example] = Seq(e1Features, e2Features).map(mkExample)
  val sequenceExamples: Seq[SequenceExample] =
    Seq(e1Features, e2Features).map(m => mkSequenceExample(m, e1FeatureList))

  private def longFeature(raw: Seq[Long]): Feature = {
    val fb = Feature.newBuilder()
    val vals = Int64List.newBuilder()
    raw.foreach(vals.addValue)
    fb.setInt64List(vals)
    fb.build
  }

  private def byteStrFeature(raw: Seq[ByteString]): Feature = {
    val fb = Feature.newBuilder()
    val vals = BytesList.newBuilder()
    raw.foreach(vals.addValue)
    fb.setBytesList(vals)
    fb.build
  }

  private def floatFeature(raw: Seq[Float]): Feature = {
    val fb = Feature.newBuilder()
    val vals = FloatList.newBuilder()
    raw.foreach(vals.addValue)
    fb.setFloatList(vals)
    fb.build
  }

  private def featureList(fs: Seq[Feature]): FeatureList =
    FeatureList
      .newBuilder()
      .addAllFeature(fs.asJava)
      .build

  private def mkExample(features: Map[String, Feature]): Example =
    Example
      .newBuilder()
      .setFeatures(Features.newBuilder().putAllFeature(features.asJava))
      .build

  private def mkSequenceExample(
    context: Map[String, Feature],
    featureList: Map[String, FeatureList]
  ): SequenceExample =
    SequenceExample
      .newBuilder()
      .setContext(Features.newBuilder().putAllFeature(context.asJava))
      .setFeatureLists(FeatureLists.newBuilder().putAllFeatureList(featureList.asJava))
      .build
}
