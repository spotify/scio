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

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.{CompletableFuture, CompletionStage, ConcurrentMap}
import java.util.function.{Consumer, Function}

import com.google.common.base.Charsets
import com.google.common.collect.Maps
import com.spotify.featran.{FeatureExtractor, MultiFeatureExtractor}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{Tap, Taps, TextTap}
import com.spotify.scio.testing.{ProtobufIO, TextIO}
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.transforms.{DoFnWithResource, JavaAsyncDoFn}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.zoltar.tf.{TensorFlowGraphModel, TensorFlowModel}
import com.spotify.zoltar.{Model, ModelLoader, Models}
import io.circe.generic.auto._
import io.circe.syntax._
import javax.annotation.Nullable
import com.twitter.algebird.{Aggregator, MultiAggregator}
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.beam.sdk.transforms.DoFn.Teardown
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.{io => gio}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.example.Example
import org.tensorflow.example.Feature.KindCase
import org.tensorflow.framework.ConfigProto
import org.tensorflow.metadata.v0._

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

private[this] abstract class PredictDoFn[T, V, M <: Model[_]](
  fetchOp: Seq[String],
  inFn: T => Map[String, Tensor[_]],
  outFn: (T, Map[String, Tensor[_]]) => V)
    extends JavaAsyncDoFn[T, V, ConcurrentMap[String, ModelLoader[M]]] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  def withResourceRunner(f: Session#Runner => V): CompletableFuture[V]

  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_CLASS

  /**
   * Process an element asynchronously.
   */
  override def processElement(input: T): CompletableFuture[V] = {
    val result: CompletionStage[V] = withResourceRunner { runner =>
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

    result.toCompletableFuture
  }

  override def createResource(): ConcurrentMap[String, ModelLoader[M]] =
    Maps.newConcurrentMap[String, ModelLoader[M]]()

}

private[tensorflow] class SavedBundlePredictDoFn[T, V](uri: String,
                                                       options: TensorFlowModel.Options,
                                                       fetchOp: Seq[String],
                                                       inFn: T => Map[String, Tensor[_]],
                                                       outFn: (T, Map[String, Tensor[_]]) => V)
    extends PredictDoFn[T, V, TensorFlowModel](fetchOp, inFn, outFn) {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  override def withResourceRunner(f: Session#Runner => V): CompletableFuture[V] =
    getResource
      .computeIfAbsent(uri, _ => Models.tensorFlow(uri, options))
      .get()
      .thenApplyAsync[V](new Function[TensorFlowModel, V] {
        override def apply(model: TensorFlowModel): V = f(model.instance().session().runner())
      })
      .toCompletableFuture

  @Teardown
  def teardown(): Unit = {
    log.info(s"Closing down predict DoFn $this")
    getResource
      .get(uri)
      .get()
      .thenAccept(new Consumer[TensorFlowModel] {
        override def accept(model: TensorFlowModel): Unit = model.close()
      })
  }

}

private[tensorflow] class GraphPredictDoFn[T, V](uri: String,
                                                 fetchOp: Seq[String],
                                                 @Nullable config: Array[Byte],
                                                 inFn: T => Map[String, Tensor[_]],
                                                 outFn: (T, Map[String, Tensor[_]]) => V)
    extends PredictDoFn[T, V, TensorFlowGraphModel](fetchOp, inFn, outFn) {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  override def withResourceRunner(f: Session#Runner => V): CompletableFuture[V] =
    getResource
      .computeIfAbsent(uri, _ => {
        val configOpt = Option(config).map(ConfigProto.parseFrom)
        Models.tensorFlowGraph(uri, configOpt.orNull, null)
      })
      .get()
      .thenApplyAsync[V](new Function[TensorFlowGraphModel, V] {
        override def apply(model: TensorFlowGraphModel): V = f(model.instance().runner())
      })
      .toCompletableFuture

  @Teardown
  def teardown(): Unit = {
    log.info(s"Closing down predict DoFn $this")
    getResource
      .get(uri)
      .get()
      .thenAccept(new Consumer[TensorFlowGraphModel] {
        override def accept(model: TensorFlowGraphModel): Unit = model.close()
      })
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
   * Save this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
   *
   * @param tFRecordSpec     TF Record description for the Examples, use the
   *                         [[com.spotify.scio.tensorflow.TFRecordSpec]] to define a description.
   * @param tfRecordSpecPath path to save the TF Record description to, by default it will be
   *                         `<PATH>/_tf_record_spec.json`
   * @group output
   */
  @deprecated("TFRecordSpec will be removed in favor of tf.metadata Schema. Use " +
    "'saveAsTfExampleFileWithMetadata'", "scio-tensorflow 0.5.6")
  def saveAsTfExampleFile(path: String,
                          tFRecordSpec: TFRecordSpec,
                          suffix: String = ".tfrecords",
                          compression: Compression = Compression.UNCOMPRESSED,
                          numShards: Int = 0,
                          tfRecordSpecPath: String = null)(
    implicit ev: T <:< Example): (Future[Tap[Example]], Future[Tap[String]]) = {
    import scala.concurrent.ExecutionContext.Implicits.global

    require(tFRecordSpec != null, "TFRecord spec can't be null")
    require(path != null, "Path can't be null")
    val _tfRecordSpecPath =
      Option(tfRecordSpecPath).getOrElse(path.replaceAll("\\/+$", "") + "/_tf_record_spec.json")

    val fi: SCollectionSeqFeatureInfo = tFRecordSpec match {
      case SeqFeatureInfo(x) =>
        SCollectionSeqFeatureInfo(self.context.parallelize(Seq(x)))
      case SCollectionSeqFeatureInfo(x) =>
        SCollectionSeqFeatureInfo(x)
    }

    import CustomCirceEncoders._
    val tfrs: SCollection[String] =
      fi.x.map(TFRecordSpecConfig(fi.LATEST_VERSION, _, compression).asJson.noSpaces)

    if (self.context.isTest) {
      self.context.testOut(TextIO(_tfRecordSpecPath))(tfrs)
      self.context.testOut(TFExampleIO(path))(self.asInstanceOf[SCollection[Example]])
      (self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Example]]],
       tfrs.saveAsInMemoryTap.asInstanceOf[Future[Tap[String]]])
    } else {
      tfrs.map { e =>
        val featureSpecResource = FileSystems.matchNewResource(_tfRecordSpecPath, false)
        val writer = FileSystems.create(featureSpecResource, MimeTypes.TEXT)
        try {
          writer.write(ByteBuffer.wrap(s"$e\n".getBytes(Charsets.UTF_8)))
        } finally {
          writer.close()
        }
      }
      val featureSpecFuture = Future(TextTap(_tfRecordSpecPath))
      val r = self.map(_.toByteArray).saveAsTfRecordFile(path, suffix, compression, numShards)
      (r.map(_.map(Example.parseFrom)), featureSpecFuture)
    }
  }

  /**
   * Save this SCollection of [[Example]] as TensorFlow TFRecord files.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.FeatureSpec]]
   * @group output
   */
  @deprecated("TFRecordSpec will be removed in favor of tf.metadata Schema. Use " +
    "'saveAsTfExampleFileWithMetadata'", "scio-tensorflow 0.5.6")
  def saveAsTfExampleFile(
    path: String,
    fe: FeatureExtractor[SCollection, _]): (Future[Tap[Example]], Future[Tap[String]]) =
    saveAsTfExampleFile(path, fe, Compression.UNCOMPRESSED)

  /**
   * Save this SCollection of [[Example]] as TensorFlow TFRecord files.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.FeatureSpec]]
   * @group output
   */
  @deprecated("TFRecordSpec will be removed in favor of tf.metadata Schema. Use " +
    "'saveAsTfExampleFileWithMetadata'", "scio-tensorflow 0.5.6")
  def saveAsTfExampleFile(path: String,
                          fe: FeatureExtractor[SCollection, _],
                          compression: Compression): (Future[Tap[Example]], Future[Tap[String]]) =
    self.saveAsTfExampleFile(path,
                             FeatranTFRecordSpec.fromFeatureSpec(fe.featureNames),
                             compression = compression)


  /**
   * Save this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file,
   * along with a protobuf file representing the inferred `org.tensorflow.metadata.v0.Schema`.
   * If you need to alter the schema, use [[inferExampleMetadata]] and pass altered schema to
   * [[saveAsTfExampleFile()]].
   * @group output
   */
  def saveAsTfExampleFile(path: String)(implicit ev: T <:< Example)
  : (Future[Tap[Example]], Future[Tap[Schema]]) = {
    this.saveAsTfExampleFile(
      path,
      schema = null,
      schemaFilename = "_schema.pb",
      suffix = ".tfrecords",
      compression = Compression.UNCOMPRESSED,
      numShards = 0
    )(ev)
  }

  def saveAsTfExampleFile(path: String,
                          schema: SCollection[Schema],
                          schemaFilename: String,
                          suffix: String,
                          compression: Compression,
                          numShards: Int)
                          (implicit ev: T <:< Example)
  : (Future[Tap[Example]], Future[Tap[Schema]]) = {
    val inferedSchema = Option(schema).getOrElse(self.inferExampleMetadata(ev))
    val schemaPath = path.replaceAll("\\/+$", "") + "/" + schemaFilename
    if (self.context.isTest) {
      self.context.testOut(TFExampleIO(path))(self.asInstanceOf[SCollection[Example]])
      self.context.testOut(ProtobufIO(schemaPath))(inferedSchema)
      (self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Example]]], inferedSchema.saveAsInMemoryTap)
    } else {
      import scala.concurrent.ExecutionContext.Implicits.global
      saveExampleMetadata(inferedSchema, schemaPath)
      val r = self.map(_.toByteArray).saveAsTfRecordFile(path, suffix, compression, numShards)
      val fExample = r.map(_.map(Example.parseFrom))
      val fSchema = fExample.map { _ =>
        val id = FileSystems.matchSingleFileSpec(schemaPath).resourceId()
        val schema = Schema.parseFrom(Channels.newInputStream(FileSystems.open(id)))
        new Tap[Schema] {
          override def value: Iterator[Schema] = Iterator(schema)
          override def open(sc: ScioContext): SCollection[Schema] = sc.parallelize(Seq(schema))
        }
      }
      (fExample, fSchema)
    }
  }

  /**
   * Infer a `org.tensorflow.metadata.v0.Schema` from this SCollection of
   * `org.tensorflow.example.Example`.
   * @return A singleton `SCollection` containing the schema.
   */
  def inferExampleMetadata(implicit ev: T <:< Example): SCollection[Schema] =
    examplesToFeatures(self.asInstanceOf[SCollection[Example]])
      .groupBy(_ => ())
      .values.map(features => Schema.newBuilder().addAllFeature(features.asJava).build())

  private def saveExampleMetadata(schema: SCollection[Schema], schemaPath: String)
  : SCollection[Schema] = {
    schema.map { s =>
      val d = FileSystems.matchNewResource(schemaPath, false)
      val chnnl = Channels.newOutputStream(FileSystems.create(d, MimeTypes.BINARY))
      try {
        s.writeTo(chnnl)
      } finally {
        chnnl.close()
      }
    }
    schema
  }

  private def examplesToFeatures(examples: SCollection[Example]): SCollection[Feature] =
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
      .aggregateByKey(MultiAggregator((Aggregator.max[Int], Aggregator.min[Int])))
      .map { case ((featureName, featureType), (max, min)) =>
        val builder = Feature.newBuilder()
          .setName(featureName)
          .setType(featureType)
        if (max == min) {
          // This is a fixed length feature
          builder.setShape(FixedShape.newBuilder()
            .addDim(FixedShape.Dim.newBuilder().setSize(max)))
        }
        else {
          // Var length
          builder.setValueCount(ValueCount.newBuilder().setMin(min).setMax(max))
        }
        builder.build()
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
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.MultiFeatureSpec]]
   * @group output
   */
  @deprecated("TFRecordSpec will be removed in favor of tf.metadata Schema. Use " +
    "'saveAsTfExampleFileWithMetadata'", "scio-tensorflow 0.5.6")
  def saveAsTfExampleFile(
    path: String,
    fe: MultiFeatureExtractor[SCollection, _]): (Future[Tap[Example]], Future[Tap[String]]) =
    saveAsTfExampleFile(path, fe, Compression.UNCOMPRESSED)

  /**
   * Merge each [[Seq]] of [[Example]] and save them as TensorFlow TFRecord files.
   * Caveat: if some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.MultiFeatureSpec]]
   * @group output
   */
  @deprecated("TFRecordSpec will be removed in favor of tf.metadata Schema. Use " +
    "'saveAsTfExampleFileWithMetadata'", "scio-tensorflow 0.5.6")
  def saveAsTfExampleFile(path: String,
                          fe: MultiFeatureExtractor[SCollection, _],
                          compression: Compression): (Future[Tap[Example]], Future[Tap[String]]) = {
    self
      .map(mergeExamples)
      .saveAsTfExampleFile(path,
                           FeatranTFRecordSpec.fromMultiSpec(fe.featureNames),
                           compression = compression)
  }

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
    if (self.context.isTest) {
      self.context.testOut(TFRecordIO(path))(self.asInstanceOf[SCollection[Array[Byte]]])
      self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Array[Byte]]]]
    } else {
      self
        .asInstanceOf[SCollection[Array[Byte]]]
        .applyInternal(
          gio.TFRecordIO
            .write()
            .to(self.pathWithShards(path))
            .withSuffix(suffix)
            .withNumShards(numShards)
            .withCompression(compression))
      self.context.makeFuture(TFRecordFileTap(ScioUtil.addPartSuffix(path)))
    }
  }

}
