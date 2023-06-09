# Tensorflow

TODO

Scio supports several methods of reading and writing [Tensorflow](https://www.tensorflow.org/) records.

## Reading TFRecord
## Writing TFRecord

## Reading TFExample
## Writing TFExample


@scaladoc[`tfRecordExampleFile`](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordExampleFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[org.tensorflow.proto.example.Example])
@scaladoc[`tfRecordExampleFileWithSchema`](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordExampleFileWithSchema(path:String,schemaFilename:String,compression:org.apache.beam.sdk.io.Compression):(com.spotify.scio.values.SCollection[org.tensorflow.proto.example.Example],com.spotify.scio.values.DistCache[org.tensorflow.metadata.v0.Schema]))
@scaladoc[`tfRecordFile`](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[Array[Byte]])
@scaladoc[`tfRecordSequenceExampleFile`](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordSequenceExampleFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[org.tensorflow.proto.example.SequenceExample])
@scaladoc[`tfRecordSequenceExampleFileWithSchema`](com.spotify.scio.tensorflow.syntax.ScioContextOps#tfRecordSequenceExampleFileWithSchema(path:String,schemaFilename:String,compression:org.apache.beam.sdk.io.Compression):(com.spotify.scio.values.SCollection[org.tensorflow.proto.example.SequenceExample],com.spotify.scio.values.DistCache[org.tensorflow.metadata.v0.Schema]))


For `SCollection[T]` where `T` is a subclass of `Example`:
@scaladoc[`saveAsTfRecordFile`](com.spotify.scio.tensorflow.syntax.ExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.Example])

For `SCollection[Seq[T]]` where `T` is a subclass of `Example`:
@scaladoc[`saveAsTfRecordFile`](com.spotify.scio.tensorflow.syntax.SeqExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.Example])

For `SCollection[T]` where `T` is a subclass of `SequenceExample`:
@scaladoc[`saveAsTfRecordFile`](com.spotify.scio.tensorflow.syntax.SequenceExampleSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[org.tensorflow.proto.example.SequenceExample])

For `SCollection[Array[Byte]]`, where it is recommended that the bytes are a serialized `Example`:
@scaladoc[`saveAsTfRecordFile`](com.spotify.scio.tensorflow.syntax.TFRecordSCollectionOps#saveAsTfRecordFile(path:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,numShards:Int,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitev:T%3C:%3CArray[Byte]):com.spotify.scio.io.ClosedTap[Array[Byte]])


# TODO PredictSCollectionOps
