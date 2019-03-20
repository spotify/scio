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

package com.spotify.scio.tensorflow.syntax

import java.nio.channels.Channels

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.tensorflow.{
  SavedBundlePredictDoFn,
  TFExampleIO,
  TFRecordIO,
  TFSequenceExampleIO
}
import com.spotify.scio.values.SCollection
import com.spotify.zoltar.tf.TensorFlowModel
import com.twitter.algebird.{Aggregator, MultiAggregator}
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.beam.sdk.util.MimeTypes
import org.tensorflow._
import org.tensorflow.example.Feature.KindCase
import org.tensorflow.example.{Example, SequenceExample}
import org.tensorflow.metadata.v0._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

object ExampleSCollectionOps {
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveExampleMetadata(schema: SCollection[Schema], schemaPath: String): Unit =
    if (!schema.context.isTest) {
      schema.map { s =>
        val d = FileSystems.matchNewResource(schemaPath, false)
        val chnnl =
          Channels.newOutputStream(FileSystems.create(d, MimeTypes.BINARY))
        try {
          s.writeTo(chnnl)
        } finally {
          chnnl.close()
        }
      }
    }

  // scalastyle:off method.length
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  private def examplesToFeatures(examples: SCollection[Example])(
    implicit coder: Coder[Feature]): SCollection[Feature] = {
    // count allows us to check presence of features, could also be used for statistics
    val countSI = examples.count.asSingletonSideInput
    examples
      .flatMap(_.getFeatures.getFeatureMap.asScala)
      .map {
        case (name, feature) =>
          feature.getKindCase match {
            case KindCase.BYTES_LIST =>
              ((name, FeatureType.BYTES), feature.getBytesList.getValueCount)
            case KindCase.FLOAT_LIST =>
              ((name, FeatureType.FLOAT), feature.getFloatList.getValueCount)
            case KindCase.INT64_LIST =>
              ((name, FeatureType.INT), feature.getInt64List.getValueCount)
            case KindCase.KIND_NOT_SET =>
              sys.error(s"kind must be set - feature is ${feature.toString}")
          }
      }
      .aggregateByKey(MultiAggregator((Aggregator.max[Int], Aggregator.min[Int], Aggregator.size)))
      .withSideInputs(countSI)
      .map {
        case (((featureName, featureType), (max, min, size)), ctx) =>
          val count = ctx(countSI)
          val builder = Feature
            .newBuilder()
            .setName(featureName)
            .setType(featureType)
          if (max == min && size == count) {
            // This is a fixed length feature, if:
            // * length of the feature list is constant
            // * feature list was present in all features

            // Presence in all the features is required for Example parsing logic in TensorFlow
            val shapeBuilder = FixedShape.newBuilder()
            if (max > 1) {
              // No need to set dim for scalars
              shapeBuilder.addDim(FixedShape.Dim.newBuilder().setSize(max))
            }
            builder.setShape(shapeBuilder)
          } else {
            // Var length feature
            builder.setValueCount(ValueCount.newBuilder().setMin(min).setMax(max))
          }
          builder.setPresence(
            FeaturePresence
              .newBuilder()
              .setMinCount(size)
              .setMinFraction(size.toFloat / count))
          builder.build()
      }
      .toSCollection
  }
  // scalastyle:on method.length
}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with TensorFlow methods.
 */
final class PredictSCollectionOps[T: ClassTag](private val self: SCollection[T]) {

  /**
   * Predict/infer/forward-pass on a TensorFlow Saved Model.
   *
   * @param savedModelUri URI of Saved TensorFlow model
   * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
   * @param options   configuration parameters for the session specified as a
   *                 `com.spotify.zoltar.tf.TensorFlowModel.Options`.
   * @param inFn     translates input elements of T to map of input-operation ->
   *                 [[org.tensorflow.Tensor Tensor]]. This method takes ownership of the
   *                 [[org.tensorflow.Tensor Tensor]]s.
   * @param outFn    translates output of prediction from map of output-operation ->
   *                 [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
   *                 ownership of the [[org.tensorflow.Tensor Tensor]]s.
   */
  def predict[V: Coder, W](savedModelUri: String,
                           fetchOps: Seq[String],
                           options: TensorFlowModel.Options)(inFn: T => Map[String, Tensor[_]])(
    outFn: (T, Map[String, Tensor[_]]) => V): SCollection[V] =
    self.parDo(new SavedBundlePredictDoFn[T, V](savedModelUri, options, fetchOps, inFn, outFn))
}

final class ExampleSCollectionOps[T <: Example](private val self: SCollection[T]) extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @return
   */
  @deprecated("saveAsTfExampleFile is deprecated: use saveAsTfRecordFile instead", "Scio 0.7.4")
  def saveAsTfExampleFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] =
    saveAsTfRecordFile(path, suffix = suffix, compression = compression, numShards = numShards)

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] = {
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[Example]].write(TFExampleIO(path))(param)
  }

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @group output
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(path: String): Future[Tap[Example]] = {
    this.saveAsTfExampleFileWithSchema(
      path,
      schema = null,
      schemaFilename = "_inferred_schema.pb"
    )
  }

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file,
   * along with  `org.tensorflow.metadata.v0.Schema`.
   * @group output
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(path: String, schema: Schema): Future[Tap[Example]] = {
    this.saveAsTfExampleFileWithSchema(
      path,
      schema,
      schemaFilename = "_schema.pb"
    )
  }

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file,
   * along with `org.tensorflow.metadata.v0.Schema`.
   * @return
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(
    path: String,
    schema: Schema,
    schemaFilename: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] = {
    require(schemaFilename != null && schemaFilename != "", "schema filename has to be set!")
    val schemaPath = path.replaceAll("\\/+$", "") + "/" + schemaFilename
    if (schema == null) {
      // by default if there is no schema provided infer and save schema
      inferExampleMetadata(schemaPath)
    } else {
      ExampleSCollectionOps
        .saveExampleMetadata(self.context.parallelize(Some(schema)), schemaPath)
    }
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[Example]].write(TFExampleIO(path))(param)
  }

  /**
   * Infer a `org.tensorflow.metadata.v0.Schema` from this SCollection of
   * `org.tensorflow.example.Example`.
   * @param schemaPath optional path to save inferred schema
   * @return A singleton `SCollection` containing the schema
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  private[tensorflow] def inferExampleMetadata(schemaPath: String = null): SCollection[Schema] = {
    implicit val sc = Coder[Schema]
    implicit val fc = Coder[Feature]

    val result = ExampleSCollectionOps
      .examplesToFeatures(self.asInstanceOf[SCollection[Example]])
      .groupBy(_ => ())
      .values
      .map(features => Schema.newBuilder().addAllFeature(features.asJava).build())
    if (schemaPath != null) {
      ExampleSCollectionOps.saveExampleMetadata(result, schemaPath)
    }
    result
  }

}

object SeqExampleSCollectionOps {

  private val mergeExamples: Seq[Example] => Example =
    _.foldLeft(Example.newBuilder)((b, i) => b.mergeFrom(i)).build()

}

final class SeqExampleSCollectionOps[T <: Example](private val self: SCollection[Seq[T]])
    extends AnyVal {

  def mergeExamples(e: Seq[Example]): Example = SeqExampleSCollectionOps.mergeExamples(e)

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  @deprecated("saveAsTfExampleFile is deprecated: use saveAsTfRecordFile instead", "Scio 0.7.4")
  def saveAsTfExampleFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] =
    saveAsTfRecordFile(path, suffix = suffix, compression = compression, numShards = numShards)

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] =
    new ExampleSCollectionOps(self.map(SeqExampleSCollectionOps.mergeExamples))
      .saveAsTfRecordFile(path, suffix, compression, numShards)

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(path: String): Future[Tap[Example]] =
    saveAsTfExampleFileWithSchema(path, schema = null, schemaFilename = "_inferred_schema.pb")

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(path: String, schema: Schema): Future[Tap[Example]] =
    saveAsTfExampleFileWithSchema(path, schema, schemaFilename = "_schema.pb")

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  @deprecated("Schema inference will be removed. We recommend using TensorFlow Data Validation",
              "Scio 0.7.0")
  def saveAsTfExampleFileWithSchema(
    path: String,
    schema: Schema,
    schemaFilename: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[Example]] =
    new ExampleSCollectionOps(self.map(this.mergeExamples))
      .saveAsTfExampleFileWithSchema(path, schema, schemaFilename, suffix, compression, numShards)

}

final class TFRecordSCollectionOps[T <: Array[Byte]](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
   * `Array[Byte]`. The recommended record encoding is `org.tensorflow.example.Example` protocol
   * buffers (which contain `org.tensorflow.example.Features` as a field) serialized as bytes.
   *
   * @group output
   */
  def saveAsTfRecordFile(path: String,
                         suffix: String = TFRecordIO.WriteParam.DefaultSuffix,
                         compression: Compression = TFRecordIO.WriteParam.DefaultCompression,
                         numShards: Int = TFRecordIO.WriteParam.DefaultNumShards)(
    implicit ev: T <:< Array[Byte]): Future[Tap[Array[Byte]]] = {
    val param = TFRecordIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[Array[Byte]]].write(TFRecordIO(path))(param)
  }

}

final class SequenceExampleSCollectionOps[T <: SequenceExample](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Saves this SCollection of `org.tensorflow.example.SequenceExample` as a TensorFlow
   * TFRecord file.
   *
   * @return
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = TFExampleIO.WriteParam.DefaultSuffix,
    compression: Compression = TFExampleIO.WriteParam.DefaultCompression,
    numShards: Int = TFExampleIO.WriteParam.DefaultNumShards): Future[Tap[SequenceExample]] = {
    val param = TFExampleIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[SequenceExample]].write(TFSequenceExampleIO(path))(param)
  }
}

trait SCollectionSyntax {

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[PredictSCollectionOps]].
   */
  implicit def tensorFlowPredictSCollectionOps[T: ClassTag](
    s: SCollection[T]): PredictSCollectionOps[T] = new PredictSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFRecordSCollectionOps]].
   */
  implicit def tensorFlowTFRecordSCollectionOps[T <: Array[Byte]](
    s: SCollection[T]): TFRecordSCollectionOps[T] = new TFRecordSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[ExampleSCollectionOps]].
   */
  implicit def tensorFlowExampleSCollectionOps[T <: Example](
    s: SCollection[T]): ExampleSCollectionOps[T] = new ExampleSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[SeqExampleSCollectionOps]].
   */
  implicit def tensorFlowSeqExampleSCollectionOps[T <: Example](
    s: SCollection[Seq[T]]): SeqExampleSCollectionOps[T] = new SeqExampleSCollectionOps(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[SequenceExampleSCollectionOps]].
   */
  implicit def tensorFlowSequenceExampleSCollectionOps[T <: SequenceExample](
    s: SCollection[T]): SequenceExampleSCollectionOps[T] = new SequenceExampleSCollectionOps(s)
}
