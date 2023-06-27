# Tensorflow

Scio supports several methods of reading and writing [Tensorflow](https://www.tensorflow.org/) records.

## Reading

Depending on your input format, and if you need to provide a schema or not, there are various ways to read Tensorflow files.

@scaladoc[tfRecordFile](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[Array[Byte]]) reads entire `TFRecord` files into byte array elements in the pipeline, @scaladoc[tfRecordExampleFile](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordExampleFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[org.tensorflow.proto.example.Example]) (or @scaladoc[tfRecordExampleFileWithSchema](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordExampleFileWithSchema(path:String,schemaFilename:String,compression:org.apache.beam.sdk.io.Compression):(com.spotify.scio.values.SCollection[org.tensorflow.proto.example.Example],com.spotify.scio.values.DistCache[org.tensorflow.metadata.v0.Schema]))) will read @javadoc[Example](org.tensorflow.proto.example.Example) instances, and @scaladoc[tfRecordSequenceExampleFile](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordSequenceExampleFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[org.tensorflow.proto.example.SequenceExample]) (or @scaladoc[tfRecordSequenceExampleFileWithSchema](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordSequenceExampleFileWithSchema(path:String,schemaFilename:String,compression:org.apache.beam.sdk.io.Compression):(com.spotify.scio.values.SCollection[org.tensorflow.proto.example.SequenceExample],com.spotify.scio.values.DistCache[org.tensorflow.metadata.v0.Schema]))) will read @javadoc[SequenceExample](org.tensorflow.proto.example.SequenceExample) instances:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.tensorflow._
import org.tensorflow.proto.example.{Example, SequenceExample}

val sc: ScioContext = ???
val recordBytes: SCollection[Array[Byte]] = sc.tfRecordFile("gs://input-record-path")
val examples: SCollection[Example] = sc.tfRecordExampleFile("gs://input-example-path")
val sequenceExamples: SCollection[SequenceExample] = sc.tfRecordSequenceExampleFile("gs://input-sequence-example-path")
```

## Writing

Similar to reading, there are multiple ways to write Tensorflow files, depending on the format of the elements to be output.
Each of these write methods is called `saveAsTfRecordFile`, but only one variant of the method is available based on the element type.

* For `SCollection[T]` where `T` is a subclass of `Example`: @scaladoc[saveAsTfRecordFile](com.spotify.scio.tensorflow.syntax.ExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.Example])
* For `SCollection[Seq[T]]` where `T` is a subclass of `Example`: @scaladoc[saveAsTfRecordFile](com.spotify.scio.tensorflow.syntax.SeqExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.Example])
* For `SCollection[T]` where `T` is a subclass of `SequenceExample`: @scaladoc[saveAsTfRecordFile](com.spotify.scio.tensorflow.syntax.SequenceExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.SequenceExample])
* For `SCollection[Array[Byte]]`, where it is recommended that the bytes are a serialized `Example`:
@scaladoc[saveAsTfRecordFile](com.spotify.scio.tensorflow.syntax.TFRecordSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitev:T%3C:%3CArray[Byte]):com.spotify.scio.io.ClosedTap[Array[Byte]])

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.tensorflow._
import org.tensorflow.proto.example.{Example, SequenceExample}

val recordBytes: SCollection[Array[Byte]] = ???
val examples: SCollection[Example] = ???
val seqExamples: SCollection[Seq[Example]] = ???
val sequenceExamples: SCollection[SequenceExample] = ???

recordBytes.saveAsTfRecordFile("gs://output-record-path")
examples.saveAsTfRecordFile("gs://output-example-path")
seqExamples.saveAsTfRecordFile("gs://output-seq-example-path")
sequenceExamples.saveAsTfRecordFile("gs://output-sequence-example-path")
```

# Prediction/inference

Scio supports preforming inference on a saved Tensorflow model.

For an `SCollection` of an arbitrary user type, predictions can be made against the raw model via @scaladoc[predict](com.spotify.scio.tensorflow.syntax.PredictSCollectionOps#predict[V,W](savedModelUri:String,fetchOps:Seq[String],options:com.spotify.zoltar.tf.TensorFlowModel.Options,signatureName:String)(inFn:T=%3EMap[String,org.tensorflow.Tensor])(outFn:(T,Map[String,org.tensorflow.Tensor])=%3EV)(implicitevidence$1:com.spotify.scio.coders.Coder[V]):com.spotify.scio.values.SCollection[V]) or using the model's [SignatureDefs](https://www.tensorflow.org/tfx/serving/signature_defs) with @scaladoc[predictWithSigDef](com.spotify.scio.tensorflow.syntax.PredictSCollectionOps#predictWithSigDef[V,W](savedModelUri:String,options:com.spotify.zoltar.tf.TensorFlowModel.Options,fetchOps:Option[Seq[String]],signatureName:String)(inFn:T=%3EMap[String,org.tensorflow.Tensor])(outFn:(T,Map[String,org.tensorflow.Tensor])=%3EV)(implicitevidence$2:com.spotify.scio.coders.Coder[V]):com.spotify.scio.values.SCollection[V]):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.tensorflow._
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow._
import org.tensorflow.proto.example.Example

case class A()
case class B()

def toTensors(a: A): Map[String, Tensor] = ???
def fromTensors(a: A, tensors: Map[String, Tensor]): B = ???

val elements: SCollection[A] = ???
val options: TensorFlowModel.Options = ???
val fetchOpts: Seq[String] = ???

val result: SCollection[B] = elements.predict[B]("gs://model-path", fetchOpts, options)(toTensors)(fromTensors)
val b: SCollection[B] = elements.predictWithSigDef[B]("gs://model-path", options)(toTensors)(fromTensors _)
```

For an `SCollection` of some subclass of `Example`, a prediction can be made via @scaladoc[predictTfExamples](com.spotify.scio.tensorflow.syntax.PredictSCollectionOps#predictTfExamples[V](savedModelUri:String,options:com.spotify.zoltar.tf.TensorFlowModel.Options,exampleInputOp:String,fetchOps:Option[Seq[String]],signatureName:String)(outFn:(T,Map[String,org.tensorflow.Tensor])=%3EV)(implicitevidence$3:com.spotify.scio.coders.Coder[V],implicitev:T%3C:%3Corg.tensorflow.proto.example.Example):com.spotify.scio.values.SCollection[V]):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.tensorflow._
import com.spotify.zoltar.tf.TensorFlowModel
import org.tensorflow._
import org.tensorflow.proto.example.Example

val exampleElements: SCollection[Example] = ???
val options: TensorFlowModel.Options = ???
def toExample(in: Example, tensors: Map[String, Tensor]): Example = ???

val c: SCollection[Example] = exampleElements.predictTfExamples[Example]("gs://model-path", options) {
  case (a, tensors) => toExample(a, tensors)
}
```
