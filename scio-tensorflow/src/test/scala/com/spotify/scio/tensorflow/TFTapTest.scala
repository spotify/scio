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

import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.io.TapSpec
import org.apache.commons.io.FileUtils
import shapeless.datatype.tensorflow._

class TFTapTest extends TapSpec {

  object TestFeatureSpec {
    val featuresType: TensorFlowType[TestFeatures] = TensorFlowType[TestFeatures]

    case class TestFeatures(f1: Float, f2: Float)

  }

  private def getDummyExample = {
    import TestFeatureSpec._
    val features = Seq(TestFeatures(1.0F, 2.0F), TestFeatures(5.0F, 3.0F))
    features.map(featuresType.toExample(_))
  }

  "SCollection" should "support saveAsTFRecordFile" in {
    val data = Seq.fill(100)(UUID.randomUUID().toString)
    import org.apache.beam.sdk.io.{Compression => CType}
    for (compressionType <- Seq(CType.UNCOMPRESSED, CType.DEFLATE, CType.GZIP)) {
      val dir = tmpDir
      val t = runWithFileFuture {
        _
          .parallelize(data)
          .map(_.getBytes)
          .saveAsTfRecordFile(dir.getPath, compression = compressionType)
      }
      verifyTap(t.map(new String(_)), data.toSet)
      FileUtils.deleteDirectory(dir)
    }
  }

  it should "support saveAsTfExampleFile with case class or Seq feature spec" in {
    val examples = getDummyExample
    import org.apache.beam.sdk.io.{Compression => CType}
    for (compressionType <- Seq(CType.UNCOMPRESSED, CType.DEFLATE, CType.GZIP)) {
      val dir = tmpDir
      val sc = ScioContext()
      val (out, spec) = sc.parallelize(examples)
        .saveAsTfExampleFile(
          dir.getPath,
          TFRecordSpec.fromCaseClass[TestFeatureSpec.TestFeatures](compressionType))
      sc.close().waitUntilDone()
      verifyTap(out.waitForResult(), examples.toSet)
      verifyTap(spec.waitForResult(), Set(
        s"""{"version":1,""" +
          """"features":[["FloatList","f2"],["FloatList","f1"]],""" +
          s""""compression":"$compressionType"}"""
      ))
      FileUtils.deleteDirectory(dir)
    }
  }

  it should "support saveAsTfExampleFile with SCollection based feature spec" in {
    val examples = getDummyExample
    import org.apache.beam.sdk.io.{Compression => CType}
    for (compressionType <- Seq(CType.UNCOMPRESSED, CType.DEFLATE, CType.GZIP)) {
      val dir = tmpDir
      val sc = ScioContext()
      val featureSpec = sc.parallelize(Option(Seq("f1", "f2")))
      val (out, spec) = sc.parallelize(examples)
        .saveAsTfExampleFile(
          dir.getPath,
          TFRecordSpec.fromFeatran(featureSpec, compressionType))
      sc.close().waitUntilDone()
      verifyTap(out.waitForResult(), examples.toSet)
      verifyTap(spec.waitForResult(), Set(
        s"""{"version":1,""" +
          """"features":[["FloatList","f1"],["FloatList","f2"]],""" +
          s""""compression":"$compressionType"}"""
      ))
      FileUtils.deleteDirectory(dir)
    }
  }

}
