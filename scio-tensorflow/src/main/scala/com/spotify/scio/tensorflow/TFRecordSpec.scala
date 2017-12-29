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

import com.spotify.scio.ScioContext
import com.spotify.scio.tensorflow.FeatureKind._
import com.spotify.scio.values.SCollection
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.beam.sdk.io.Compression

/** Contains metadata about TFRecords. Useful to read the records later on. */
trait TFRecordSpec {
  val LATEST_VERSION: Int = 1
  def toJson(sc: ScioContext, compression: Compression): SCollection[String]
}

/** Information necessary to extract a given feature in TF. */
final case class FeatureInfo(name: String, kind: FeatureKind, tags: Map[String, String])

/** TFRecord metadata that will end up serialized. */
final case class TFRecordSpecConfig(version: Int,
                                    features: Seq[FeatureInfo],
                                    compression: Compression) {

  // Circe struggles with Java Enum, the easiest workaround is to turn all Enums into Strings.
  case class Str(version: Int,
                 features: Seq[(String, String, Map[String, String])],
                 compression: String)


  def asJson: String = Str(
    version,
    features.map(t => (t.name, t.kind.toString, t.tags)),
    compression.toString
  ).asJson.noSpaces

}

private final case class SeqFeatureInfo(x: Seq[FeatureInfo]) extends TFRecordSpec {
  override def toJson(sc: ScioContext, compression: Compression): SCollection[String] =
    sc.parallelize(Seq(TFRecordSpecConfig(LATEST_VERSION, x, compression).asJson))
}

object TFRecordSpec {

  import scala.reflect.runtime.universe._

  /** Infer the TFRecordSpec from a case class. */
  def fromCaseClass[T: TypeTag]: TFRecordSpec = {
    require(typeOf[T].typeSymbol.isClass && typeOf[T].typeSymbol.asClass.isCaseClass,
      "Type must be a case class")

    val s = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => FeatureInfo(
        m.name.decodedName.toString,
        FeatureKind(m.returnType),
        Map())
    }.toSeq
    SeqFeatureInfo(s)
  }
}
