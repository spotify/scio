/*
 * Copyright 2017 Spotify AB.
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

import com.spotify.scio.tensorflow.FeatureKind._
import com.spotify.scio.values.SCollection
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.beam.sdk.io.Compression

/**
 * Metadata about TFRecords. Useful to read the records later on.
 */
object TFRecordSpec {

  import scala.reflect.runtime.universe._

  /**
   * Infer TFRecordSpec from case class (Using default compression).
   *
   * @tparam T Case class to use for TFRecordSpec inference
   * @return Inferred TFRecordSpec
   */
  def fromCaseClass[T: TypeTag]: TFRecordSpec = fromCaseClass(Compression.DEFLATE)


  /**
   * Infer TFRecordSpec from case class.
   *
   * @param compression Type of [[org.apache.beam.sdk.io.Compression]] used to save these TFRecords
   * @tparam T Case class to use for TFRecordSpec inference
   * @return Inferred TFRecordSpec
   */
  def fromCaseClass[T: TypeTag](compression: Compression): TFRecordSpec = {
    require(typeOf[T].typeSymbol.isClass && typeOf[T].typeSymbol.asClass.isCaseClass,
      "Type must be a case class")

    val s = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => FeatureInfo(
        m.name.decodedName.toString,
        FeatureKind(m.returnType),
        Map())
    }.toSeq
    CaseClassTFRecordSpec(s, compression)
  }

  /**
   * Infer TFRecordSpec from Featran's featureNames.
   *
   * @param featureNames Feature names
   * @param compression  Type of [[org.apache.beam.sdk.io.Compression]] used to save these
   *                     TFRecords
   * @return Inferred TFRecordSpec
   */
  def fromFeatureSpec(featureNames: SCollection[Seq[String]],
                      compression: Compression = Compression.DEFLATE): TFRecordSpec = {
    FeatranTFRecordSpec(
      featureNames.map(_.map(n => FeatureInfo(n, FeatureKind.FloatList, Map()))),
      compression)
  }

  /**
   * Infer TFRecordSpec from Featran's featureNames (For MultiSpec).
   *
   * @param featureNames Feature names
   * @param compression  Type of [[org.apache.beam.sdk.io.Compression]] used to save these
   *                     TFRecords
   * @return Inferred TFRecordSpec
   */
  def fromMultiSpec(featureNames: SCollection[Seq[Seq[String]]],
                    compression: Compression = Compression.DEFLATE): TFRecordSpec = {
    val featureInfos = featureNames.map(_.zipWithIndex.flatMap {
      case (sss, i) => sss.map(n =>
        FeatureInfo(n, FeatureKind.FloatList, Map("multispec-id" -> i.toString)))
    })
    FeatranTFRecordSpec(featureInfos, compression)
  }
}


sealed trait TFRecordSpec {
  def compression: Compression
}

private final case class FeatureInfo(name: String, kind: FeatureKind, tags: Map[String, String])

private final case class CaseClassTFRecordSpec(x: Seq[FeatureInfo],
                                               compression: Compression) extends TFRecordSpec

private final case class FeatranTFRecordSpec(x: SCollection[Seq[FeatureInfo]],
                                             compression: Compression) extends TFRecordSpec

private final case class TFRecordSpecConfig(version: Int,
                                            features: Seq[FeatureInfo],
                                            compression: Compression) {

  // Circe struggles with Java Enum, the easiest is to turn all Enums into Strings.
  case class Str(version: Int,
                 features: Seq[(String, String, Map[String, String])],
                 compression: String)


  def asJson: String = Str(
    version,
    features.map(t => (t.name, t.kind.toString, t.tags)),
    compression.toString
  ).asJson.noSpaces

}



