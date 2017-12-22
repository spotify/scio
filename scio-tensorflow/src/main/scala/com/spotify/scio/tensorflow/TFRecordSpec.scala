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

object TFRecordSpec {

  import scala.reflect.runtime.universe._


  def fromCaseClass[T: TypeTag]: TFRecordSpec = fromCaseClass(Compression.DEFLATE)

  def fromCaseClass[T: TypeTag](compression: Compression): TFRecordSpec = {
    require(typeOf[T].typeSymbol.isClass && typeOf[T].typeSymbol.asClass.isCaseClass,
      "Type must be a case class")

    val s = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => (
        FeatureKind(m.returnType),
        m.name.decodedName.toString)
    }.toSeq
    CaseClassTFRecordSpec(s, compression)
  }

  def fromFeatran(featureNames: SCollection[Seq[String]],
                  compression: Compression = Compression.DEFLATE): TFRecordSpec = {
    FeatranTFRecordSpec(
      featureNames.map(_.map(n => (FeatureKind.FloatList, n))),
      compression)
  }
}


sealed trait TFRecordSpec {
  def compression: Compression
}

final case class CaseClassTFRecordSpec(x: Seq[(FeatureKind, String)],
                                       compression: Compression) extends TFRecordSpec

final case class FeatranTFRecordSpec(x: SCollection[Seq[(FeatureKind, String)]],
                                     compression: Compression) extends TFRecordSpec

private final case class TFRecordSpecConfig(version: Int,
                                            features: Seq[(FeatureKind, String)],
                                            compression: Compression) {

  // Circe struggles with Compression (Java Enum)
  case class Str(version: Int,
                 features: Seq[(String, String)],
                 compression: String)


  def asJson: String = Str(version,
    features.map(t => (t._1.toString, t._2)),
    compression.toString
  ).asJson.noSpaces

}



