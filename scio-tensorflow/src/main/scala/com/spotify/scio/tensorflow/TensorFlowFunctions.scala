/*
 * Copyright 2018 Spotify AB.
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

import java.nio.channels.Channels
import java.time.Duration
import java.util.concurrent.ConcurrentMap
import java.util.function.Function

import com.google.common.collect.Maps
import com.spotify.scio.io.Tap
import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.zoltar.tf.{TensorFlowGraphModel, TensorFlowModel}
import com.spotify.zoltar.{Model, Models}
import com.twitter.algebird.{Aggregator, MultiAggregator}
import javax.annotation.Nullable
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Teardown}
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.{io => gio}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.example.Example
import org.tensorflow.example.Feature.KindCase
import org.tensorflow.framework.ConfigProto
import org.tensorflow.metadata.v0._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

private[this] abstract class PredictDoFn[T, V, M <: Model[_]](
  fetchOp: Seq[String],
  inFn: T => Map[String, Tensor[_]],
  outFn: (T, Map[String, Tensor[_]]) => V)
    extends DoFnWithResource[T, V, ConcurrentMap[String, M]] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  def withRunner(f: Session#Runner => V): V

  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLASS

  /**
   * Process an element asynchronously.
   */
  @ProcessElement
  def processElement(c: DoFn[T, V]#ProcessContext): Unit = {
    val result = withRunner { runner =>
      val input = c.element()
      val i = inFn(input)
      var result: V = null.asInstanceOf[V]

      try {
        i.foreach { case (op, t) => runner.feed(op, t) }
        fetchOp.foreach(runner.fetch)
        val outTensors = runner.run()
        try {
          import scala.collection.breakOut
          result = outFn(input, (fetchOp zip outTensors.asScala)(breakOut))
        } finally {
          log.debug("Closing down output tensors")
          outTensors.asScala.foreach(_.close())
        }
      } finally {
        log.debug("Closing down input tensors")
        i.foreach { case (_, t) => t.close() }
      }

      result
    }

    c.output(result)
  }

  override def createResource(): ConcurrentMap[String, M] =
    Maps.newConcurrentMap[String, M]()

}

private[tensorflow] class SavedBundlePredictDoFn[T, V](uri: String,
                                                       options: TensorFlowModel.Options,
                                                       fetchOp: Seq[String],
                                                       inFn: T => Map[String, Tensor[_]],
                                                       outFn: (T, Map[String, Tensor[_]]) => V)
    extends PredictDoFn[T, V, TensorFlowModel](fetchOp, inFn, outFn) {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  override def withRunner(f: Session#Runner => V): V = {
    val model = getResource
      .computeIfAbsent(
        uri,
        new Function[String, TensorFlowModel] {
          override def apply(t: String): TensorFlowModel =
            Models
              .tensorFlow(uri, options)
              .get(Duration.ofDays(Integer.MAX_VALUE))
        }
      )

    f(model.instance().session().runner())
  }

  @Teardown
  def teardown(): Unit = {
    log.info(s"Tearing down predict DoFn $this")
    getResource.get(uri).close()
  }

}

private[tensorflow] class GraphPredictDoFn[T, V](uri: String,
                                                 fetchOp: Seq[String],
                                                 @Nullable config: Array[Byte],
                                                 inFn: T => Map[String, Tensor[_]],
                                                 outFn: (T, Map[String, Tensor[_]]) => V)
    extends PredictDoFn[T, V, TensorFlowGraphModel](fetchOp, inFn, outFn) {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  override def withRunner(f: Session#Runner => V): V = {
    val model = getResource
      .computeIfAbsent(
        uri,
        new Function[String, TensorFlowGraphModel] {
          override def apply(t: String): TensorFlowGraphModel = {
            val configOpt = Option(config).map(ConfigProto.parseFrom)
            Models
              .tensorFlowGraph(uri, configOpt.orNull, null)
              .get(Duration.ofDays(Integer.MAX_VALUE))
          }
        }
      )

    f(model.instance().runner())
  }

  @Teardown
  def teardown(): Unit = {
    log.info(s"Tearing down predict DoFn $this")
    getResource.get(uri).close()
  }
}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with TensorFlow methods.
 */
private[tensorflow] class PredictSCollectionFunctions[T: ClassTag](
  @transient val self: SCollection[T])
    extends Serializable {

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
  def predict[V: ClassTag, W](savedModelUri: String,
                              fetchOps: Seq[String],
                              options: TensorFlowModel.Options)(inFn: T => Map[String, Tensor[_]])(
    outFn: (T, Map[String, Tensor[_]]) => V): SCollection[V] =
    self.parDo(new SavedBundlePredictDoFn[T, V](savedModelUri, options, fetchOps, inFn, outFn))

  /**
   * Predict/infer/forward-pass on TensorFlow Graph.
   *
   * @param graphUri URI of Graph TensorFlow model
   * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
   * @param config   configuration parameters for the session specified as a serialized
   *                 `org.tensorflow.framework.ConfigProto` protocol buffer.
   * @param inFn     translates input elements of T to map of input-operation ->
   *                 [[org.tensorflow.Tensor Tensor]]. This method takes ownership of the
   *                 [[org.tensorflow.Tensor Tensor]]s.
   * @param outFn    translates output of prediction from map of output-operation ->
   *                 [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
   *                 ownership of the [[org.tensorflow.Tensor Tensor]]s.
   */
  @deprecated("TensorFlow Graph model support will be removed. Use Saved Model predict.",
              "scio-tensorflow 0.5.4")
  def predict[V: ClassTag, W](graphUri: String, fetchOps: Seq[String], config: Array[Byte] = null)(
    inFn: T => Map[String, Tensor[_]])(outFn: (T, Map[String, Tensor[_]]) => V): SCollection[V] =
    self.parDo(new GraphPredictDoFn[T, V](graphUri, fetchOps, config, inFn, outFn))

}

class TFExampleSCollectionFunctions[T <: Example](val self: SCollection[T]) {

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   * @group output
   */
  def saveAsTfExampleFile(path: String): Future[Tap[Example]] = {
    this.saveAsTfExampleFile(
      path,
      schema = null,
      schemaFilename = "_inferred_schema.pb",
      suffix = ".tfrecords",
      compression = Compression.UNCOMPRESSED,
      numShards = 0
    )
  }

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file,
   * along with  `org.tensorflow.metadata.v0.Schema`.
   * @group output
   */
  def saveAsTfExampleFile(path: String, schema: Schema)
  : Future[Tap[Example]] = {
    this.saveAsTfExampleFile(
      path,
      schema,
      schemaFilename = "_schema.pb",
      suffix = ".tfrecords",
      compression = Compression.UNCOMPRESSED,
      numShards = 0
    )
  }

  /**
   * Saves this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file,
   * along with `org.tensorflow.metadata.v0.Schema`.
   * @return
   */
  def saveAsTfExampleFile(path: String,
                          schema: Schema,
                          schemaFilename: String,
                          suffix: String,
                          compression: Compression,
                          numShards: Int): Future[Tap[Example]] = {
    require(schemaFilename != null && schemaFilename != "", "schema filename has to be set!")
    val schemaPath = path.replaceAll("\\/+$", "") + "/" + schemaFilename
    if (schema == null) {
      // by default if there is no schema provided infer and save schema
      self.inferExampleMetadata(schemaPath)
    } else {
      // TODO (#1252): maybe enforce some schema checks, but at least save the schema along the data
      TFExampleSCollectionFunctions
        .saveExampleMetadata(self.context.parallelize(Some(schema)), schemaPath)
    }
    val param = TFExampleIO.WriteParam(suffix,compression, numShards)
    self.asInstanceOf[SCollection[Example]].write(TFExampleIO(path))(param)
  }

  /**
   * Infer a `org.tensorflow.metadata.v0.Schema` from this SCollection of
   * `org.tensorflow.example.Example`.
   * @param schemaPath optional path to save infered schema
   * @return A singleton `SCollection` containing the schema
   */
  def inferExampleMetadata(schemaPath: String = null)
  : SCollection[Schema] = {
    val result = examplesToFeatures(self.asInstanceOf[SCollection[Example]])
      .groupBy(_ => ())
      .values.map(features => Schema.newBuilder().addAllFeature(features.asJava).build())
    if (schemaPath != null) {
      TFExampleSCollectionFunctions.saveExampleMetadata(result, schemaPath)
    }
    result
  }

  // scalastyle:off method.length
  private def examplesToFeatures(examples: SCollection[Example]): SCollection[Feature] = {
    // count allows us to check presence of features, could also be used for statistics
    val countSI = examples.count.asSingletonSideInput
    examples
      .flatMap(_.getFeatures.getFeatureMap.asScala)
      .map { case (name, feature) =>
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
      .map { case (((featureName, featureType), (max, min, size)), ctx) =>
        val count = ctx(countSI)
        val builder = Feature.newBuilder()
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
        builder.setPresence(FeaturePresence.newBuilder()
          .setMinCount(size)
          .setMinFraction(size.toFloat/count))
        builder.build()
      }.toSCollection
  }
  // scalastyle:on method.length
}

private object TFExampleSCollectionFunctions {
  def saveExampleMetadata(schema: SCollection[Schema], schemaPath: String): Unit =
    if (!schema.context.isTest) {
      schema.map { s =>
        val d = FileSystems.matchNewResource(schemaPath, false)
        val chnnl = Channels.newOutputStream(FileSystems.create(d, MimeTypes.BINARY))
        try {
          s.writeTo(chnnl)
        } finally {
          chnnl.close()
        }
      }
    }
}

class SeqTFExampleSCollectionFunctions[T <: Example](@transient val self: SCollection[Seq[T]])
    extends Serializable {

  def mergeExamples(e: Seq[Example]): Example =
    e.foldLeft(Example.newBuilder)((b, i) => b.mergeFrom(i))
      .build()

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfExampleFile(path: String): Future[Tap[Example]] = {
    this.saveAsTfExampleFile(
      path,
      schema = null,
      schemaFilename = "_inferred_schema.pb",
      suffix = ".tfrecords",
      compression = Compression.UNCOMPRESSED,
      numShards = 0
    )
  }

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfExampleFile(path: String, schema: Schema)
  : Future[Tap[Example]] = {
    this.saveAsTfExampleFile(
      path,
      schema,
      schemaFilename = "_schema.pb",
      suffix = ".tfrecords",
      compression = Compression.UNCOMPRESSED,
      numShards = 0
    )
  }

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @group output
   */
  def saveAsTfExampleFile(path: String,
                          schema: Schema,
                          schemaFilename: String,
                          suffix: String,
                          compression: Compression,
                          numShards: Int): Future[Tap[Example]] =
    self
      .map(this.mergeExamples)
      .saveAsTfExampleFile(path, schema, schemaFilename, suffix, compression, numShards)

}

class TFRecordSCollectionFunctions[T <: Array[Byte]](val self: SCollection[T]) {

  /**
   * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
   * `Array[Byte]`. The recommended record encoding is `org.tensorflow.example.Example` protocol
   * buffers (which contain `org.tensorflow.example.Features` as a field) serialized as bytes.
   *
   * @group output
   */
  def saveAsTfRecordFile(
    path: String,
    suffix: String = ".tfrecords",
    compression: Compression = Compression.UNCOMPRESSED,
    numShards: Int = 0)(implicit ev: T <:< Array[Byte]): Future[Tap[Array[Byte]]] = {
    val param = TFRecordIO.WriteParam(suffix, compression, numShards)
    self.asInstanceOf[SCollection[Array[Byte]]].write(TFRecordIO(path))(param)
  }

}
