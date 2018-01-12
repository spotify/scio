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

import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.Compression

/** Contains metadata about TFRecords. Useful to read the records later on. */
sealed trait TFRecordSpec {
  val LATEST_VERSION: Int = 1
}

private final case class SCollectionSeqFeatureInfo(@transient x: SCollection[Seq[FeatureInfo]])
  extends TFRecordSpec

private final case class SeqFeatureInfo(x: Seq[FeatureInfo]) extends TFRecordSpec

private final case class TFRecordSpecConfig(version: Int,
                                            features: Seq[FeatureInfo],
                                            compression: Compression)

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

  /** Pass useful information about each Feature. */
  private[scio] def fromSColSeqFeatureInfo(x: SCollection[Seq[FeatureInfo]]): TFRecordSpec =
    SCollectionSeqFeatureInfo(x)
}

private object FeatranTFRecordSpec {
  def fromFeatureSpec(featureNames: SCollection[Seq[String]]): TFRecordSpec = {
    TFRecordSpec.fromSColSeqFeatureInfo(
      featureNames.map(_.map(n => FeatureInfo(n, FeatureKind.FloatList, Map()))))
  }

  def fromMultiSpec(featureNames: SCollection[Seq[Seq[String]]]): TFRecordSpec = {
    val featureInfos = featureNames.map(_.zipWithIndex.flatMap {
      case (sss, i) => sss.map(n =>
        FeatureInfo(n, FeatureKind.FloatList, Map("multispec-id" -> i.toString)))
    })
    TFRecordSpec.fromSColSeqFeatureInfo(featureInfos)
  }
}
