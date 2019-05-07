# Protobuf

## Read Protobuf files

Scio comes with custom and efficient support for reading Protobuf files via `protobufFile` method, for example:

```scala mdoc:reset
val sc = "yolo"
println(sc)
```


```scala
// FooBarProto is a Protobuf generated class (must be a subclass of Protobuf's `Message`)
sc.protobufFile[FooBarProto]("gs://path-to-data/lake/part-*.protobuf.avro")
  .map( message => ??? )
// `message` is of type FooBarProto
```

Important: in most cases the input files should have been previously written by Scio. The reason is that Scio assumes that serialized Protobuf message is stored inside `bytes` Avro record.

If you want to read serialized protobuf messages directly from a file, one solution is to use `textFile` followed by a `map` to parse your messages.

## Write Protobuf files

Scio comes with custom and efficient support for writing Protobuf files via `saveAsProtobufFile` method, for example:

```scala
// FooBarProto is a Protobuf generated class (must be a subclass of Protobuf's `Message`)
sc.map { x =>
    // build a protobuf message>
    ???
  }
  .saveAsProtobufFile("gs://path-to-data/lake/protos-out")
```

## File format

Scio's Protobuf file is backed by Avro with the following schema:

```json
{
  "type" : "record",
  "name" : "AvroBytesRecord",
  "fields" : [ {
    "name" : "bytes",
    "type" : "bytes"
  } ]
}
```

Avro gives us a block based file format with compression, split and combine support. Protobuf binary is stored in the `bytes` field of `AvroBytesRecord`.

Starting with Scio 0.2.6, the Protobuf schema also is stored as a JSON string in the Avro file metadata under the key `protobuf.generic.schema`. You can get the schema or JSON records using the `proto-tools` command line tool from [gcs-tools](https://github.com/spotify/gcs-tools) (available in our homebrew tap). Conversion between Protobuf schema, binary and JSON is done via the [protobuf-generic](https://github.com/nevillelyh/protobuf-generic) library.

```
brew tap spotify/public
brew install gcs-proto-tools
proto-tools getschema data.protobuf.avro
proto-tools tojson data.protobuf.avro
```

## Common issues

### ScalaPB

If you end up using [ScalaPB](https://github.com/trueaccord/ScalaPB), make sure to use java based message class as input/output type, Scala based message class does not inherit from Protobuf's Message class. To generate both Scala and Java classes add (to your `build.sbt`):

```scala
import com.trueaccord.scalapb.{ScalaPbPlugin => PB}
PB.javaConversions in PB.protobufConfig := true
```
